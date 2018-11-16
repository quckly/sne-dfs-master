package pw.quckly.sne.dfs.master

import pw.quckly.sne.dfs.master.api.*
import ru.serce.jnrfuse.ErrorCodes

/***
 * The main class of DFS master server
 *
 * Implements logic to communicate with slaves and clients though API.
 */
class DfsMaster {

    // https://github.com/SerCeMan/jnr-fuse/blob/master/src/main/java/ru/serce/jnrfuse/examples/MemoryFS.java

    val rootDir = MemoryDirectory("", this)

    // Clients methods

    fun read(request: ReadRequest): ReadResponse {

    }

    fun write(request: WriteRequest): StatusResponse {

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

        if (path == null)
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

        when (path) {
            null -> {
                throw DfsException(ErrorCodes.ENOENT())
            }
            is MemoryDirectory -> {
                // Add pseudo directories
                val contents = arrayOf(".", "..") + path.contents()
                return ReadDirResponse(OK_RESPONSE, contents)
            }
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
                path.rename(request.to)
            }
            else -> {
                return StatusResponse(ErrorCodes.ENOTDIR())
            }
        }

        return StatusResponse(ErrorCodes.ENOENT())
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
    }
}
