package pw.quckly.sne.dfs.master

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean

@SpringBootApplication
class SneDfsMasterApplication {

//    @Bean
//    fun createDfsMaster(): DfsMaster {
//        return DfsMaster()
//    }
}

fun main(args: Array<String>) {
    runApplication<SneDfsMasterApplication>(*args)
}
