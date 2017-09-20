package kjkrol.kafkademo.consumer

import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration


fun main(args: Array<String>) {
    SpringApplication.run(VideoContentConsumerApp::class.java, *args)
}

@SpringBootApplication(exclude = arrayOf(DataSourceAutoConfiguration::class))
class VideoContentConsumerApp

