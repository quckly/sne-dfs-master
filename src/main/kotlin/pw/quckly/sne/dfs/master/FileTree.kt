package pw.quckly.sne.dfs.master

import java.util.ArrayList

class MemoryDirectory : MemoryPath {
    private val contents = ArrayList<MemoryPath>()

    constructor(name: String) : super(name) {}

    constructor(name: String, parent: MemoryDirectory) : super(name, parent) {}

    @Synchronized
    fun add(p: MemoryPath) {
        contents.add(p)
        p.parent = this
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
        contents.add(MemoryDirectory(lastComponent, this))
    }

    @Synchronized
    fun mkfile(lastComponent: String) {
        contents.add(MemoryFile(lastComponent, this))
    }

    @Synchronized
    fun contents(): Array<String> {
        return arrayOf()
    }
}

class MemoryFile(name: String, parent: MemoryDirectory) : MemoryPath(name, parent) {

    fun read(size: Long, offset: Long): ByteArray {
        return byteArrayOf()
    }

    @Synchronized
    fun truncate(size: Long) {
    }

    fun write(data: ByteArray, offset: Long) {}
}

abstract class MemoryPath {
    var name: String
        protected set

    var parent: MemoryDirectory?

    constructor(name: String, parent: MemoryDirectory? = null) {
        this.name = name
        this.parent = parent
    }

    @Synchronized
    fun delete() {
        if (parent != null) {
            parent!!.deleteChild(this)
            parent = null
        }
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
        var newName = to
        while (newName.startsWith("/")) {
            newName = newName.substring(1)
        }
        name = newName
    }
}