package pw.quckly.sne.dfs.master

import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import pw.quckly.sne.dfs.master.api.*
import ru.serce.jnrfuse.ErrorCodes
import java.lang.IllegalStateException
import java.util.*
import java.util.concurrent.CopyOnWriteArrayList

class SlaveServer(val id: Int,
                  val guid: String,
                  val chunkCount: Int,
                  var serverAddress: String,
                  var serverPort: Int) {

    var freeChunkCount = chunkCount
    var chunks = BitSet(chunkCount)
    var available = true

    fun allocateChunk(): Int {
        if (!hasSpace()) {
            throw IllegalStateException("No available space in this server.")
        }

        val nextFreeChunk = chunks.nextClearBit(0)
        chunks.set(nextFreeChunk)
        --freeChunkCount

        return nextFreeChunk
    }

    fun freeChunk(chunkId: Int) {
        chunks.clear(chunkId)
        ++freeChunkCount
    }

    fun hasSpace() = freeChunkCount > 0

    fun getUrl() = "http://${serverAddress}:${serverPort}"
}

/***
 * The main class of DFS master server
 *
 * Implements logic to communicate with slaves and clients though API.
 *
 * Some code taken from https://github.com/SerCeMan/jnr-fuse/blob/master/src/main/java/ru/serce/jnrfuse/examples/MemoryFS.java
 */

// TODO: Client LOGS
// TODO: DIRECTORY SIZE, ROOT FREE SPACE
// TODO: Redistribute files OR replicate
@Component
class DfsMaster {

    @Value("\${app.requests.timeout}")
    var requestTimeout = 2000

    // Used to map file chunks to <serverId;chunkId> pair
    var lastServerId = 0;
    // In memory FS
    val rootDir = MemoryDirectory("", this)
    // In-consistency slave servers
    val servers = CopyOnWriteArrayList<SlaveServer>()

    // Slave methods

    fun registerSlave(request: SlaveRegisterRequest, serverAddress: String) {
        val existenServer = getServerByGuid(request.guid)

        // Check existing one
        if (existenServer != null) {
            // Some checks
            if (request.chunksCount == existenServer.chunkCount) {
                // Restore server from shutdown
                existenServer.available = true

                // Server IP and port can be changed
                existenServer.serverAddress = serverAddress
                existenServer.serverPort = request.port
                return
            }
        }

        // Add a new server
        val newServer = SlaveServer(lastServerId++,
                request.guid,
                request.chunksCount,
                serverAddress,
                request.port)

        servers.add(newServer)
    }

    // Clients methods

    /**
     * Read data no more than one chunk
     */
    fun read(request: ReadRequest): ReadResponse {
        val path = getPath(request.path)

        when (path) {
            null -> {
                throw DfsException(ErrorCodes.ENOENT())
            }
            !is MemoryFile -> {
                throw DfsException(ErrorCodes.EISDIR())
            }
            else -> {
                val data = path.read(request.offset, request.size)
                val b64data = b64encoder.encode(data).toString(Charsets.UTF_8)

                return ReadResponse(OK_RESPONSE, b64data)
            }
        }
    }

    /**
     * Write data no more than one chunk
     */
    fun write(request: WriteRequest): StatusResponse {
        val path = getPath(request.path)

        when (path) {
            null -> {
                return StatusResponse(ErrorCodes.ENOENT())
            }
            !is MemoryFile -> {
                return StatusResponse(ErrorCodes.EISDIR())
            }
            else -> {
                lateinit var data: ByteArray

                try {
                    data = b64decoder.decode(request.data)
                } catch (e: IllegalArgumentException) {
                    throw DfsException(ErrorCodes.EIO(), e.message ?: "Base64 error")
                }

                path.write(request.offset, data)

                return StatusResponse(OK_RESPONSE)
            }
        }
    }

    // Utils

    fun allocateChunk(): FileChunk {
        val serverCandidates = servers
                // First insert data to more free servers
                .filter { it.available && it.hasSpace() }
                .sortedByDescending { it.freeChunkCount }
                .take(REPLICA_COUNT)

        if (serverCandidates.size < REPLICA_COUNT) {
            throw DfsException(ErrorCodes.ENOSPC(), "No such REPLICA count available")
        }

        val fileChunk = FileChunk()

        for (server in serverCandidates) {
            fileChunk.mapping.add(server.id to server.allocateChunk())
        }

        return fileChunk
    }

    fun freeChunk(fileChunk: FileChunk) {
        for (serverChunk in fileChunk.mapping) {
            getServerById(serverChunk.first)?.freeChunk(serverChunk.second)
        }
    }

    fun getServerByGuid(guid: String): SlaveServer? {
        return servers.find { it.guid == guid }
    }

    fun getServerById(id: Int): SlaveServer? {
        return servers.find { it.id == id }
    }

    // Create file
    fun create(request: FilePathRequest): StatusResponse {
        val path = getPath(request.path)

        if (path != null)
            throw DfsException(ErrorCodes.EEXIST())

        val parent = getParentPath(request.path)

        if (parent == null)
            return StatusResponse(ErrorCodes.ENOENT())

        if (parent is MemoryDirectory) {
            parent.mkfile(getLastComponent(request.path))

            return StatusResponse(OK_RESPONSE)
        }

        return StatusResponse(ErrorCodes.ENOTDIR())
    }

    fun getattr(request: FilePathRequest): AttrResponse {
        val path = getPath(request.path)

        if (path == null)
            throw DfsException(ErrorCodes.ENOENT())

        return path.getAttr()
    }

    fun mkdir(request: FilePathRequest): StatusResponse {
        val path = getPath(request.path)

        if (path != null)
            return StatusResponse(ErrorCodes.EEXIST())

        val parent = getParentPath(request.path)

        if (parent == null)
            return StatusResponse(ErrorCodes.ENOENT())

        if (parent is MemoryDirectory) {
            parent.mkdir(getLastComponent(request.path))

            return StatusResponse(OK_RESPONSE)
        }

        return StatusResponse(ErrorCodes.ENOTDIR())
    }

    fun readdir(request: ReadDirRequest): ReadDirResponse {
        val path = getPath(request.path)

        if (path == null)
            throw DfsException(ErrorCodes.ENOENT())

        if (path is MemoryDirectory) {
            // Add pseudo directories
            val contents = arrayOf(".", "..") + path.contents()
            return ReadDirResponse(OK_RESPONSE, contents)
        }

        throw DfsException(ErrorCodes.ENOTDIR())
    }

    /**
     * Move file or directory
     */
    fun rename(request: RenameRequest): StatusResponse {
        val path = getPath(request.from)

        if (path == null)
            return StatusResponse(ErrorCodes.ENOENT())

        val newParent = getParentPath(request.to)

        when (newParent) {
            null -> {
                return StatusResponse(ErrorCodes.ENOENT())
            }
            is MemoryDirectory -> {
                path.changeParent(newParent)
                path.rename(getLastComponent(request.to))

                return StatusResponse(OK_RESPONSE)
            }
            else -> {
                return StatusResponse(ErrorCodes.ENOTDIR())
            }
        }
    }

    fun rmdir(request: FilePathRequest): StatusResponse {
        val path = getPath(request.path)

        when (path) {
            null -> {
                return StatusResponse(ErrorCodes.ENOENT())
            }
            is MemoryDirectory -> {
                if (!path.isEmpty()) {
                    return StatusResponse(ErrorCodes.ENOTEMPTY())
                }

                path.delete()
                return StatusResponse(OK_RESPONSE)
            }
            else -> {
                return StatusResponse(ErrorCodes.ENOTDIR())

            }
        }
    }

    fun truncate(request: TruncateRequest): StatusResponse {
        val path = getPath(request.path)

        when (path) {
            null -> {
                return StatusResponse(ErrorCodes.ENOENT())
            }
            !is MemoryFile -> {
                return StatusResponse(ErrorCodes.EISDIR())
            }
            else -> {
                path.truncate(request.size)
                return StatusResponse(OK_RESPONSE)
            }
        }
    }

    fun unlink(request: FilePathRequest): StatusResponse {
        val path = getPath(request.path)

        when (path) {
            null -> {
                return StatusResponse(ErrorCodes.ENOENT())
            }
            !is MemoryFile -> {
                return StatusResponse(ErrorCodes.EISDIR())
            }
            else -> {
                path.delete()
                return StatusResponse(OK_RESPONSE)
            }
        }
    }

    fun open(request: FilePathRequest): StatusResponse {
        return StatusResponse(OK_RESPONSE)
    }

    // Utils

    private fun getLastComponent(path: String): String {
        var path = path
        while (path.substring(path.length - 1) == "/") {
            path = path.substring(0, path.length - 1)
        }

        if (path.isEmpty()) {
            return ""
        } else {
            return path.substring(path.lastIndexOf("/") + 1)
        }
    }

    private fun getParentPath(path: String): MemoryPath? {
        return rootDir.find(path.substring(0, path.lastIndexOf("/")))
    }

    private fun getPath(path: String): MemoryPath? {
        return rootDir.find(path)
    }

    companion object {
        val OK_RESPONSE = 0

        val CHUNK_SIZE = 4 * 1024

        val REPLICA_COUNT = 2

        val b64encoder = Base64.getEncoder()
        val b64decoder = Base64.getDecoder()
    }
}
