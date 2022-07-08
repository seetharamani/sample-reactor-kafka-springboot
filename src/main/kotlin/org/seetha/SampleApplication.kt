package org.seetha

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
open class SampleApplication {
    fun main(args: Array<String>) {
        runApplication<SampleApplication>(*args)
    }
}