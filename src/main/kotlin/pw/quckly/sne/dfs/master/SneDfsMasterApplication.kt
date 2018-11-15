package pw.quckly.sne.dfs.master

import java.lang.reflect.Method
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.web.servlet.mvc.method.RequestMappingInfo
import org.springframework.web.servlet.mvc.condition.PatternsRequestCondition
import org.springframework.web.bind.annotation.RestController
import org.springframework.core.annotation.AnnotationUtils
import com.fasterxml.jackson.databind.util.ClassUtil.getDeclaringClass
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerMapping
import org.springframework.web.servlet.config.annotation.WebMvcConfigurationSupport

@SpringBootApplication
class SneDfsMasterApplication {

    @Bean
    fun createDfsMaster(): DfsMaster {
        return DfsMaster()
    }
}

fun main(args: Array<String>) {
    runApplication<SneDfsMasterApplication>(*args)
}

//@Configuration
//class WebConfig : WebMvcConfigurationSupport() {
//
//    override fun createRequestMappingHandlerMapping(): RequestMappingHandlerMapping {
//        return ApiAwareRequestMappingHandlerMapping()
//    }
//
//    private class ApiAwareRequestMappingHandlerMapping : RequestMappingHandlerMapping() {
//
//        protected override fun registerHandlerMethod(handler: Any, method: Method, mapping: RequestMappingInfo) {
//            var mapping = mapping
//            val beanType = method.getDeclaringClass()
//            if (AnnotationUtils.findAnnotation<RestController>(beanType, RestController::class.java) != null) {
//                val apiPattern = PatternsRequestCondition(API_PATH_PREFIX)
//                        .combine(mapping.patternsCondition)
//
//                mapping = RequestMappingInfo(mapping.name, apiPattern, mapping.methodsCondition,
//                        mapping.paramsCondition, mapping.headersCondition, mapping.consumesCondition,
//                        mapping.producesCondition, mapping.customCondition)
//            }
//            super.registerHandlerMethod(handler, method, mapping)
//        }
//
//        companion object {
//            private val API_PATH_PREFIX = "api"
//        }
//    }
//}
