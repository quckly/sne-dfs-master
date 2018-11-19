package pw.quckly.sne.dfs.master.api

data class StorageWriteRequest(val chunkId: Int, val offset: Int, val data: String)
data class StorageReadRequest(val chunkId: Int, val offset: Int, val size: Int)
data class StorageReadResponse(val data: String)
