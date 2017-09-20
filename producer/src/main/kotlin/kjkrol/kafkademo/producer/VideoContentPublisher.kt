package kjkrol.kafkademo.producer

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.kafka.core.KafkaTemplate

internal class VideoContentPublisher(private val template: KafkaTemplate<String, VideoContent>) {

    private companion object {
        val log: Logger = LoggerFactory.getLogger(VideoContentPublisher::class.java)
    }

    internal fun publish(videoContent: VideoContent): Boolean {
        log.info("publishing {}", videoContent)
        return template.sendDefault(videoContent).isDone
    }

}
