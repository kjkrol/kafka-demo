package kjkrol.kafkademo

import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication

@SpringBootApplication
class KafkaDemoApplication

fun main(args: Array<String>) {
    SpringApplication.run(KafkaDemoApplication::class.java, *args)
}
