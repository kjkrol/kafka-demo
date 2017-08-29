package kjkrol.kafkademo.consumer

import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication


fun main(args: Array<String>) {
    SpringApplication.run(VideoContentConsumerApp::class.java, *args)
}

@SpringBootApplication
class VideoContentConsumerApp

