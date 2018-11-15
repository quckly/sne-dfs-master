package pw.quckly.sne.dfs.master.api

import java.lang.Exception

class DfsException : Exception {
    val errorCode: Int
    var errorMessage: String? = null

    constructor(errorCode: Int) : super("FS Error #$errorCode") {
        this.errorCode = errorCode
    }

    constructor(errorCode: Int, errorMessage: String) : super("FS Error #$errorCode ($errorMessage)") {
        this.errorCode = errorCode
        this.errorMessage = errorMessage
    }
}
