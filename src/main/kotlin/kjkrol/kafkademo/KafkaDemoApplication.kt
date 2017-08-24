package kjkrol.kafkademo

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.boot.CommandLineRunner
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.context.annotation.Bean
import java.util.UUID
import java.util.concurrent.TimeUnit


fun main(args: Array<String>) {
    SpringApplication.run(KafkaDemoApplication::class.java, *args)
}

@SpringBootApplication
class KafkaDemoApplication {

    private companion object {
        val log: Logger = LoggerFactory.getLogger(KafkaDemoApplication::class.java)
    }

    @Bean
    internal fun init(producer: Producer, consumer: Consumer): CommandLineRunner {
        return CommandLineRunner {
            log.info("Publishing events...")
            producer.publish(VideoContent(UUID.randomUUID(), "Conan The Barbarian"))
            producer.publish(VideoContent(UUID.randomUUID(), "Terminator"))
            producer.publish(VideoContent(UUID.randomUUID(), "Predator"))
            log.info("Receiving events...")
            consumer.latch.await(60, TimeUnit.SECONDS)
            log.info("All events received.")
        }
    }

}

