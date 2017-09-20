package kjkrol.kafkademo.producer

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.boot.CommandLineRunner
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration
import org.springframework.context.annotation.Bean
import java.util.UUID


fun main(args: Array<String>) {
    SpringApplication.run(VideoContentProducerApp::class.java, *args)
}

@SpringBootApplication(exclude = arrayOf(DataSourceAutoConfiguration::class))
class VideoContentProducerApp {

    private companion object {
        val log: Logger = LoggerFactory.getLogger(VideoContentProducerApp::class.java)
    }

    @Bean
    internal fun init(producer: VideoContentPublisher): CommandLineRunner = CommandLineRunner {
        log.info("Publishing events...")
        producer.publish(VideoContent(UUID.randomUUID(), "Conan The Barbarian"))
        producer.publish(VideoContent(UUID.randomUUID(), "Terminator"))
        producer.publish(VideoContent(UUID.randomUUID(), "Predator"))
        log.info("All events published.")
    }

}

