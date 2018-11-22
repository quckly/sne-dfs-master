package pw.quckly.sne.dfs.master

import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import com.github.kittinunf.fuel.Fuel
import com.github.kittinunf.fuel.jackson.responseObject
import com.github.kittinunf.result.Result
import pw.quckly.sne.dfs.master.api.*
import ru.serce.jnrfuse.ErrorCodes
import ru.serce.jnrfuse.struct.FileStat
import java.util.*
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.collections.ArrayList

class FileChunk {
    // Mapping ServerID -> ChunkID
    var mapping = ArrayList<Pair<Int, Int>>()

    constructor()

    constructor(mapping: List<Pair<Int, Int>>) {
        this.mapping = ArrayList(mapping)
    }
}

class MemoryDirectory : MemoryPath {

    @JsonIgnore
    private val dfs: DfsMaster
    var contents = ArrayList<MemoryPath>()
        private set

    constructor(name: String, dfs: DfsMaster) : super(name) {
        this.dfs = dfs
    }

    constructor(name: String, dfs: DfsMaster, contents: List<MemoryPath>) : super(name) {
        this.dfs = dfs
        this.contents = ArrayList(contents)
    }

    constructor(name: String, parent: MemoryDirectory, dfs: DfsMaster) : super(name, parent) {
        this.dfs = dfs
    }

    fun fixParentOfContents() {
        val contentsSnapshot = this.contents.toList()

        for (path in contentsSnapshot) {
            path.changeParent(this)

            if (path is MemoryDirectory) {
                path.fixParentOfContents()
            }
        }
    }

    @Synchronized
    fun addChild(p: MemoryPath) {
        contents.add(p)
    }

    @Synchronized
    fun deleteChild(child: MemoryPath) {
        contents.remove(child)
    }

    override fun find(path: String): MemoryPath? {
        var pathToFind = path

        if (super.find(pathToFind) != null) {
            return super.find(pathToFind)
        }
        while (pathToFind.startsWith("/")) {
            pathToFind = pathToFind.substring(1)
        }
        synchronized(this) {
            if (!pathToFind.contains("/")) {
                for (p in contents) {
                    if (p.name == pathToFind) {
                        return p
                    }
                }
                return null
            }
            val nextName = pathToFind.substring(0, pathToFind.indexOf("/"))
            val rest = pathToFind.substring(pathToFind.indexOf("/"))
            for (p in contents) {
                if (p.name == nextName) {
                    return p.find(rest)
                }
            }
        }
        return null
    }

    @Synchronized
    fun mkdir(lastComponent: String) {
        contents.add(MemoryDirectory(lastComponent, this, dfs))
    }

    @Synchronized
    fun mkfile(lastComponent: String) {
        contents.add(MemoryFile(lastComponent, this, dfs))
    }

    @Synchronized
    fun contents(): Array<String> {
        return contents.map { it.name }.toTypedArray()
    }

    override fun delete() {
        if (!isEmpty())
            throw DfsException(ErrorCodes.ENOTEMPTY())

        val oldParent = parent
        if (oldParent != null) {
            oldParent.deleteChild(this)
        }
    }

    @JsonIgnore
    fun isEmpty(): Boolean {
        return contents.isEmpty()
    }

    @JsonIgnore
    override fun getAttr(): AttrResponse {
        // 0777 == 0x1FF
        return AttrResponse(0, FileStat.S_IFDIR or 0x1FF, 0, -1, -1)
    }
}

class MemoryFile(name: String, parent: MemoryDirectory?, @JsonIgnore val dfs: DfsMaster) : MemoryPath(name, parent) {

    var size: Long = 0
    var chunks = CopyOnWriteArrayList<FileChunk>()

    fun read(offset: Long, size: Long): ByteArray {
        // No more one chunk and size > 0
        if (!checkBoundaries(offset, size)) {
            throw DfsException(ErrorCodes.EIO(), "You can read no more one chunk.")
        }

        // Find chunk
        val fileChunkId = getFileChunkByOffset(offset)

        // Check end of chunks
        if (fileChunkId > (chunks.size - 1)) {
            throw DfsException(DfsMaster.OK_RESPONSE, "End of file.")
        }

        // Determine last read byte and count
        val rightBorder = Math.min(offset + size - 1, this.size - 1)
        val bytesToRead = Math.max((rightBorder - offset + 1).toInt(), 0)

        if (bytesToRead == 0) {
            return byteArrayOf()
        }

        val chunk = getFileChunkById(fileChunkId)

        // Read from server in random order for load-balancing purposes
        // But requests can fail
        val shuffledChunkMappings = chunk.mapping.shuffled()

        // Try read from any server
        chunkMappingLoop@ for ((serverId, serverChunkId) in shuffledChunkMappings) {
            val server = dfs.getServerById(serverId) ?: throw DfsException(ErrorCodes.EIO(), "Fatal error")

            if (!server.available) {
                continue@chunkMappingLoop
            }

            val readRequest = StorageReadRequest(serverChunkId,
                    (offset % DfsMaster.CHUNK_SIZE).toInt(),
                    bytesToRead)
            val readRequestJson = jsonMapper.writeValueAsString(readRequest)

            val (fuelRequest, fuelResponse, fuelResult) = Fuel.post("${server.getUrl()}/storage/read")
                    .jsonBody(readRequestJson)
                    .timeout(dfs.requestTimeout)
                    .timeoutRead(dfs.requestTimeout)
                    .responseObject<StorageReadResponse>()

            when (fuelResult) {
                is Result.Success -> {
                    val response = fuelResult.value
                    try {
                        val data = b64decoder.decode(response.data)
                        return data
                    } catch (e: IllegalArgumentException) {
                        continue@chunkMappingLoop
                    }
                }
                is Result.Failure -> {
                    // TODO: Some checks?
                    server.available = false

                    continue@chunkMappingLoop
                }
            }
        }

        throw DfsException(ErrorCodes.EIO(), "Cannot read from any servers")
    }

    @Synchronized
    fun write(offset: Long, data: ByteArray) {
        val size = data.size

        if (!checkBoundaries(offset, size.toLong())) {
            throw DfsException(ErrorCodes.EIO(), "You can write no more one chunk.")
        }

        // Find chunk that we need to write
        // It can be already used chunk or new
        val rightBorder = offset + size - 1

        val fileChunkId = getFileChunkByOffset(offset)

        // Allow to write only to next unused chunk
        if (fileChunkId > (chunks.size - 1) + 1) {
            throw DfsException(ErrorCodes.EIO(), "You can write to consequentially chunk.")
        }

        // Allocate new chunk if needs
        if (fileChunkId > (chunks.size - 1)) {
            allocateChunk()
        }

        val chunk = getFileChunkById(fileChunkId)

        // Check servers availability
        for ((serverId, serverChunkId) in chunk.mapping) {
            val server = dfs.getServerById(serverId) ?: throw DfsException(ErrorCodes.EIO(), "Fatal error")

            // If Server which we want to write is unvailable
            // Put this chunk in READ-ONLY mode
            if (!server.available) {
                throw DfsException(ErrorCodes.EINVAL(), "Cannot write to chunk that is linked to unavailable server.")
            }
        }

        // Collect statuses of all write operations
        val statuses = BooleanArray(chunk.mapping.size)

        var mappingId = 0
        for ((serverId, serverChunkId) in chunk.mapping) {
            val server = dfs.getServerById(serverId) ?: throw DfsException(ErrorCodes.EIO(), "Fatal error")

            val b64data = b64encoder.encode(data).toString(Charsets.UTF_8)
            val writeRequest = StorageWriteRequest(serverChunkId,
                    (offset % DfsMaster.CHUNK_SIZE).toInt(),
                    b64data)
            val writeRequestJson = jsonMapper.writeValueAsString(writeRequest)

            val (fuelRequest, fuelResponse, fuelResult) = Fuel.post("${server.getUrl()}/storage/write")
                    .jsonBody(writeRequestJson)
                    .timeout(dfs.requestTimeout)
                    .timeoutRead(dfs.requestTimeout)
                    .responseString() // Blocking

            when (fuelResult) {
                is Result.Success -> {
                    statuses[mappingId] = true
                }
                is Result.Failure -> {
                    statuses[mappingId] = false

                    // TODO: Some checks?
                    server.available = false
                }
            }

            ++mappingId
        }

        // Check results
        val successChunksCount = statuses.count { it }
        if (successChunksCount == chunk.mapping.size) {
            // All written

            // Change file size
            this.size = Math.max(this.size, rightBorder + 1)
        } else if (successChunksCount > 0) {
            // At least one
            // TODO: Some logic maybe
            throw DfsException(ErrorCodes.EINVAL(), "Some chunks have not written")

        } else {
            // No one
            // TODO: Some logic maybe
            throw DfsException(ErrorCodes.EINVAL(), "Cannot write to any chunks")
        }
    }

    @Synchronized
    fun truncate(size: Long) {
        // TODO: Free unused chunks

        this.size = size
        // So, do nothing, why not?
    }

    @Synchronized
    override fun delete() {
        chunks.forEach { dfs.freeChunk(it) }
        chunks.clear()
        size = 0

        val oldParent = parent
        if (oldParent != null) {
            oldParent.deleteChild(this)
        }
    }

    @JsonIgnore
    override fun getAttr(): AttrResponse {
        // 0777 == 0x1FF
        return AttrResponse(0, FileStat.S_IFREG or 0x1FF, size, -1, -1)
    }

    fun allocateChunk() {
        val chunk = dfs.allocateChunk()

        chunks.add(chunk)
    }

    fun checkBoundaries(offset: Long, size: Long): Boolean {
        val l = offset
        val r = offset + size - 1

        return size > 0 && (getFileChunkByOffset(l) == getFileChunkByOffset(r))
    }

    fun getFileChunkByOffset(offset: Long) = (offset / DfsMaster.CHUNK_SIZE).toInt()

    fun getFileChunkById(id: Int): FileChunk {
        return chunks[id]
    }

    companion object {
        val jsonMapper = ObjectMapper().registerKotlinModule()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

        val b64encoder = Base64.getEncoder()
        val b64decoder = Base64.getDecoder()
    }
}

abstract class MemoryPath {
    var name: String
        protected set

    @JsonIgnore
    var parent: MemoryDirectory?
        protected set

    constructor(name: String, parent: MemoryDirectory? = null) {
        this.name = name
        this.parent = parent
    }

    @Synchronized
    fun changeParent(newParent: MemoryDirectory) {
        newParent.addChild(this)

        val oldParent = parent
        if (oldParent != null) {
            oldParent.deleteChild(this)
        }

        parent = newParent
    }

    open fun find(path: String): MemoryPath? {
        var pathToFind = path
        while (pathToFind.startsWith("/")) {
            pathToFind = pathToFind.substring(1)
        }
        return if (pathToFind == name || pathToFind.isEmpty()) {
            this
        } else null
    }

    fun rename(to: String) {
        // TODO: CHECK
        var newName = to
        while (newName.startsWith("/")) {
            newName = newName.substring(1)
        }
        name = newName
    }

    abstract fun delete()

    @JsonIgnore
    abstract fun getAttr(): AttrResponse
}
