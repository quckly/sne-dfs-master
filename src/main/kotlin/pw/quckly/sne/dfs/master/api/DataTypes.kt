package pw.quckly.sne.dfs.master.api

// Common data structures
data class FilePathRequest(val path: String)
data class StatusResponse(val status: Int)
data class IntResponse(val status: Int, val value: Int)

// Per operation
data class ReadRequest(val path: String, val size: Long, val offset: Long)
data class ReadResponse(val status: Int, val data: String)

data class WriteRequest(val path: String, val offset: Long, val data: String)

data class ReadDirRequest(val path: String)
data class ReadDirResponse(val status: Int, val contents: Array<String>)

data class RenameRequest(val from: String, val to: String, val exchange: Boolean)
