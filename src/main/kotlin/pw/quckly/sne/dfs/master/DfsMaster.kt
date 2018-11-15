package pw.quckly.sne.dfs.master

import pw.quckly.sne.dfs.master.api.*

/***
 * The main class of DFS master server
 *
 * Implements logic to communicate with slaves and clients though API.
 */
class DfsMaster {

    // Clients methods

    fun read(request: ReadRequest): ReadResponse {

    }

    fun write(request: WriteRequest): StatusResponse {

    }

    fun create(request: FilePathRequest): StatusResponse {

    }

    fun getattr(request: FilePathRequest): IntResponse {

    }

    fun mkdir(request: FilePathRequest): StatusResponse {

    }

    fun readdir(request: ReadDirRequest): ReadDirResponse {

    }

    fun rename(request: RenameRequest): StatusResponse {

    }

    fun rmdir(request: FilePathRequest): StatusResponse {

    }

    fun truncate(request: FilePathRequest): StatusResponse {

    }

    fun unlink(request: FilePathRequest): StatusResponse {

    }

    fun open(request: FilePathRequest): StatusResponse {

    }
}
