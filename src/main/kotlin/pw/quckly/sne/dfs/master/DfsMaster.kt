package pw.quckly.sne.dfs.master

import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.core.TreeNode
import com.fasterxml.jackson.databind.*
import com.fasterxml.jackson.module.kotlin.readValue
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import com.fasterxml.jackson.module.kotlin.treeToValue
import com.github.kittinunf.fuel.jackson.mapper
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import pw.quckly.sne.dfs.master.api.*
import ru.serce.jnrfuse.ErrorCodes
import java.io.File
import java.lang.Exception
import java.lang.IllegalStateException
import java.util.*
import java.util.concurrent.CopyOnWriteArrayList
import javax.annotation.PostConstruct
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.databind.node.ArrayNode
import kotlin.collections.ArrayList

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

    @JsonIgnore
    fun getUrl() = "http://${serverAddress}:${serverPort}"
}

class FileChunkDeserializer : JsonDeserializer<FileChunk>() {

    var jsonMapper: ObjectMapper? = null

    override fun deserialize(p: JsonParser?, ctxt: DeserializationContext?): FileChunk {
        if (p == null || ctxt == null)
            throw Exception()

        val jsonMapper = jsonMapper ?: throw Exception("jsonMapper is not initialized")

        val node = p.readValueAsTree<JsonNode>()

        var mappingResult = ArrayList<Pair<Int, Int>>()
        val mappingNode = node.get("mapping") as ArrayNode

        for (m in mappingNode) {
            val first: Int = m.get("first").asInt()
            val second: Int = m.get("second").asInt()

            mappingResult.add(first to second)
        }

        return FileChunk(mappingResult)
    }
}

class MemoryFileDeserializer(val dfsMaster: DfsMaster) : JsonDeserializer<MemoryFile>() {

    var jsonMapper: ObjectMapper? = null

    override fun deserialize(p: JsonParser?, ctxt: DeserializationContext?): MemoryFile {
        if (p == null || ctxt == null)
            throw Exception()

        val jsonMapper = jsonMapper ?: throw Exception("jsonMapper is not initialized")

        val node = p.readValueAsTree<JsonNode>()

        val name: String = jsonMapper.treeToValue(node.get("name"))
        val size: Long = jsonMapper.treeToValue(node.get("size"))

        val chunks = ArrayList<FileChunk>()
        val chunksNode = node.get("chunks") as ArrayNode

        for (c in chunksNode) {
            chunks.add(jsonMapper.treeToValue(c))
        }

        val file = MemoryFile(name, null, dfsMaster)
        file.chunks = CopyOnWriteArrayList(chunks)
        file.size = size

        return file
    }
}

class MemoryDirectoryDeserializer(val dfsMaster: DfsMaster) : JsonDeserializer<MemoryDirectory>() {

    var jsonMapper: ObjectMapper? = null

    override fun deserialize(p: JsonParser?, ctxt: DeserializationContext?): MemoryDirectory {
        if (p == null || ctxt == null)
            throw Exception()

        val jsonMapper = jsonMapper ?: throw Exception("jsonMapper is not initialized")

        val node = p.readValueAsTree<JsonNode>()

        val name: String = jsonMapper.treeToValue(node.get("name"))
        val contentsNode = node.get("contents") as ArrayNode

        val contents = ArrayList<MemoryPath>()

        for (cpath in contentsNode) {
            if (cpath.has("contents")) {
                val dir: MemoryDirectory = jsonMapper.treeToValue(cpath)
                contents.add(dir)
            } else {
                val file: MemoryFile = jsonMapper.treeToValue(cpath)
                contents.add(file)
            }
        }

        return MemoryDirectory(name, dfsMaster, contents)
    }
}

/***
 * The main class of DFS master server
 *
 * Implements logic to communicate with slaves and clients though API.
 *
 * Some code taken from https://github.com/SerCeMan/jnr-fuse/blob/master/src/main/java/ru/serce/jnrfuse/examples/MemoryFS.java
 */

@Component
class DfsMaster {

    @Value("\${app.requests.timeout}")
    var requestTimeout = 2000
    @Value("\${app.metainfo.location}")
    var metainfoLocation: String? = null

    // Used to map file chunks to <serverId;chunkId> pair
    var lastServerId = 0;
    // In memory FS
    var rootDir = MemoryDirectory("", this)
    // In-consistency slave servers
    val servers = CopyOnWriteArrayList<SlaveServer>()

    @PostConstruct
    fun init() {
        if (metainfoLocation != null) {
            try {
                val module = SimpleModule()
                val memoryDirectoryDeserializer = MemoryDirectoryDeserializer(this)
                val memoryFileDeserializer = MemoryFileDeserializer(this)
                val fileChunkDeserializer = FileChunkDeserializer()
                module.addDeserializer(MemoryDirectory::class.java, memoryDirectoryDeserializer)
                module.addDeserializer(MemoryFile::class.java, memoryFileDeserializer)
                module.addDeserializer(FileChunk::class.java, fileChunkDeserializer)

                val jsonMapper = ObjectMapper().registerKotlinModule()
                        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                jsonMapper.registerModule(module)

                memoryDirectoryDeserializer.jsonMapper = jsonMapper
                memoryFileDeserializer.jsonMapper = jsonMapper
                fileChunkDeserializer.jsonMapper = jsonMapper

                var dfsState = jsonMapper.readValue(File(metainfoLocation), DfsMasterState::class.java)

                for (server in dfsState.servers) {
                    server.available = false
                }

                lastServerId = dfsState.lastServerId
                rootDir = dfsState.fsRoot
                servers.addAll(dfsState.servers)
            } catch (e: Exception) {
                logger.error("Error on reading a DFS meta information", e)
            }
        }
    }

    fun saveDfsState() {
        val metainfoLocation = metainfoLocation
        if (metainfoLocation == null) {
            logger.error("You must select metainfo file location.")
        }

        val serializableObject = DfsMasterState(lastServerId,
                rootDir,
                servers)

        try {
            mapper.writeValue(File(metainfoLocation), serializableObject)
        } catch (e: Exception) {
            logger.error("Error on try to save metainfo", e)
        }
    }

    // Slave methods

    fun registerSlave(request: SlaveRegisterRequest, serverAddress: String) {
        val existenServer = getServerByGuid(request.guid)

        // Check existing one
        if (existenServer != null) {
            // Some checks
            if (request.chunksCount == existenServer.chunkCount) {
                if (!existenServer.available) {
                    logger.info("Storage server #${existenServer.id} (${existenServer.guid}) is available.")
                }

                // Restore server from shutdown
                existenServer.available = true

                // Server IP and port can be changed
                existenServer.serverAddress = serverAddress
                existenServer.serverPort = request.port

                saveDfsState()

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

        saveDfsState()

        logger.info("New storage server #${newServer.id} is registered. (${serverAddress}:${request.port}) Guid = ${request.guid}, ChunksCount = ${request.chunksCount}")
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

                saveDfsState()

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

        saveDfsState()

        return fileChunk
    }

    fun freeChunk(fileChunk: FileChunk) {
        for (serverChunk in fileChunk.mapping) {
            getServerById(serverChunk.first)?.freeChunk(serverChunk.second)
        }

        saveDfsState()
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

            saveDfsState()

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

            saveDfsState()

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

        val targetPath = getPath(request.to)

        if (targetPath != null)
            return StatusResponse(ErrorCodes.EEXIST())

        val newParent = getParentPath(request.to)

        when (newParent) {
            null -> {
                return StatusResponse(ErrorCodes.ENOENT())
            }
            is MemoryDirectory -> {
                path.changeParent(newParent)
                path.rename(getLastComponent(request.to))

                saveDfsState()

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

                saveDfsState()

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

                saveDfsState()

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

                saveDfsState()

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

        val logger = LoggerFactory.getLogger(DfsMaster::class.java)

        val b64encoder = Base64.getEncoder()
        val b64decoder = Base64.getDecoder()
    }
}
