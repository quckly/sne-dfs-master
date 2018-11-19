package pw.quckly.sne.dfs.master

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import com.github.kittinunf.fuel.Fuel
import com.github.kittinunf.result.Result
import pw.quckly.sne.dfs.master.api.AttrResponse
import pw.quckly.sne.dfs.master.api.DfsException
import pw.quckly.sne.dfs.master.api.StorageWriteRequest
import ru.serce.jnrfuse.ErrorCodes
import ru.serce.jnrfuse.struct.FileStat
import java.util.*
import java.util.concurrent.CopyOnWriteArrayList

class FileChunk {
    // Mapping ServerID -> ChunkID
    var mapping = ArrayList<Pair<Int, Int>>()
}

class MemoryDirectory : MemoryPath {

    private val dfs: DfsMaster
    private val contents = ArrayList<MemoryPath>()

    constructor(name: String, dfs: DfsMaster) : super(name) {
        this.dfs = dfs
    }

    constructor(name: String, parent: MemoryDirectory, dfs: DfsMaster) : super(name, parent) {
        this.dfs = dfs
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

    fun isEmpty(): Boolean {
        return contents.isEmpty()
    }

    override fun getAttr(): AttrResponse {
        // 0777 == 0x1FF
        return AttrResponse(0, FileStat.S_IFDIR or 0x1FF, 0, -1, -1)
    }
}

class MemoryFile(name: String, parent: MemoryDirectory, val dfs: DfsMaster) : MemoryPath(name, parent) {

    var size: Long = 0
    var chunks = CopyOnWriteArrayList<FileChunk>()

    fun read(offset: Long, size: Long): ByteArray {
        if (!checkBoundaries(offset, size)) {
            throw DfsException(ErrorCodes.EIO(), "You can read no more one chunk.")
        }


        return ByteArray(size.toInt(), { 0x37 })
        TODO("not implemented")
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

        val chunkId = getFileChunkByOffset(offset)

        // Allow to write only to next unused chunk
        if (chunkId > (chunks.size - 1) + 1) {
            throw DfsException(ErrorCodes.EIO(), "You can write to consequentially chunk.")
        }

        // Allocate new chunk if needs
        if (chunkId > (chunks.size - 1)) {
            allocateChunk()
        }

        val chunk = getFileChunkById(chunkId)

        for (serverChunk in chunk.mapping) {
            val server = dfs.getServerById(serverChunk.first) ?: throw DfsException(ErrorCodes.EIO(), "Fatal error")

            val b64data = b64encoder.encode(data).toString(Charsets.UTF_8)
            val writeRequest = StorageWriteRequest(serverChunk.second,
                    (offset % DfsMaster.CHUNK_SIZE).toInt(),
                    b64data)
            val writeRequestJson = jsonMapper.writeValueAsString(writeRequest)

            val fuelResponse = Fuel.post("http://${server.serverAddress}:${server.serverPort}/write")
                    .jsonBody(writeRequestJson)
                    .responseString()

            when (fuelResponse.third) {
                is Result.Success -> {

                }
                is Result.Failure -> {

                }
            }
        }


        TODO("not implemented")
    }

    @Synchronized
    fun truncate(size: Long) {
        TODO("not implemented")
    }

    @Synchronized
    override fun delete() {
        chunks.forEach { dfs.freeChunk(it) }
        chunks.clear()
        size = 0
    }

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
    abstract fun getAttr(): AttrResponse
}
