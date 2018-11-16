package pw.quckly.sne.dfs.master

import pw.quckly.sne.dfs.master.api.AttrResponse
import pw.quckly.sne.dfs.master.api.DfsException
import ru.serce.jnrfuse.ErrorCodes
import ru.serce.jnrfuse.struct.FileStat
import java.util.ArrayList

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
    var chunks = arrayOf<FileChunk>()

    fun read(size: Long, offset: Long): ByteArray {
        return byteArrayOf()
    }

    @Synchronized
    fun truncate(size: Long) {
        TODO("not implemented")
    }

    @Synchronized
    fun write(data: ByteArray, offset: Long) {
        TODO("not implemented")
    }

    @Synchronized
    override fun delete() {
        TODO("not implemented")
    }

    override fun getAttr(): AttrResponse {
        // 0777 == 0x1FF
        return AttrResponse(0, FileStat.S_IFREG or 0x1FF, size, -1, -1)
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