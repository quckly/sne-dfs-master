package pw.quckly.sne.dfs.master.api

import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*
import org.springframework.web.context.request.WebRequest
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler
import pw.quckly.sne.dfs.master.DfsMaster
import javax.servlet.http.HttpServletRequest

@RestController
class DfsHttpApiController(val dfsMaster: DfsMaster) {

    @PostMapping("/master/register")
    fun registerSlave(@RequestBody request: SlaveRegisterRequest, servletRequest: HttpServletRequest) {
        dfsMaster.registerSlave(request, servletRequest.remoteAddr)
    }

    // Controllers for POSIX FUSE Client

    @PostMapping("/fs/read")
    fun read(@RequestBody request: ReadRequest): ReadResponse {
        return dfsMaster.read(request)
    }

    @PostMapping("/fs/write")
    fun write(@RequestBody request: WriteRequest): StatusResponse {
        return dfsMaster.write(request)
    }

    @PostMapping("/fs/create")
    fun create(@RequestBody request: FilePathRequest): StatusResponse {
        return dfsMaster.create(request)
    }

    @PostMapping("/fs/getattr")
    fun getattr(@RequestBody request: FilePathRequest): AttrResponse {
        return dfsMaster.getattr(request)
    }

    @PostMapping("/fs/mkdir")
    fun mkdir(@RequestBody request: FilePathRequest): StatusResponse {
        return dfsMaster.mkdir(request)
    }

    @PostMapping("/fs/readdir")
    fun readdir(@RequestBody request: ReadDirRequest): ReadDirResponse {
        return dfsMaster.readdir(request)
    }

    @PostMapping("/fs/rename")
    fun rename(@RequestBody request: RenameRequest): StatusResponse {
        return dfsMaster.rename(request)
    }

    @PostMapping("/fs/rmdir")
    fun rmdir(@RequestBody request: FilePathRequest): StatusResponse {
        return dfsMaster.rmdir(request)
    }

    @PostMapping("/fs/truncate")
    fun truncate(@RequestBody request: TruncateRequest): StatusResponse {
        return dfsMaster.truncate(request)
    }

    @PostMapping("/fs/unlink")
    fun unlink(@RequestBody request: FilePathRequest): StatusResponse {
        return dfsMaster.unlink(request)
    }

    @PostMapping("/fs/open")
    fun open(@RequestBody request: FilePathRequest): StatusResponse {
        return dfsMaster.open(request)
    }

    @ControllerAdvice
    class RestResponseEntityExceptionHandler : ResponseEntityExceptionHandler() {

        @ExceptionHandler(DfsException::class)
        fun handleDfsExceptions(ex: Exception, request: WebRequest): ResponseEntity<StatusResponse> {
            if (ex is DfsException) {
                logger.error("FS API Error: ${ex.message}")

                val response = StatusResponse(ex.errorCode,
                        ex.errorMessage)

                return ResponseEntity(response, HttpHeaders(), HttpStatus.BAD_REQUEST)
            }

            logger.error("Strange error", ex)

            val response = StatusResponse(-1,
                    ex.message)

            return ResponseEntity(response, HttpHeaders(), HttpStatus.BAD_REQUEST)
        }

        @ExceptionHandler(Exception::class)
        fun handleExceptions(ex: Exception, request: WebRequest): ResponseEntity<StatusResponse> {
            ex.printStackTrace()

            val response = StatusResponse(-1,
                    ex.message)

            return ResponseEntity(response, HttpHeaders(), HttpStatus.BAD_REQUEST)
        }
    }
}
