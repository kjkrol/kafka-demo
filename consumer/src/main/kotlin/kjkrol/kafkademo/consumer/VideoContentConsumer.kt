package kjkrol.kafkademo.consumer

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.Acknowledgment


class VideoContentConsumer {

    private companion object {
        val log: Logger = LoggerFactory.getLogger(VideoContentConsumer::class.java)
    }

    @KafkaListener(topics = arrayOf("\${app.kafka.consumer.topic}"), containerFactory = "kafkaListenerContainerFactory")
    internal fun listen(consumerRecords: ConsumerRecord<String, VideoContent>, ack: Acknowledgment) {
        log.info("consuming {}", consumerRecords)
        val isFine: Boolean = true
        if (isFine) ack.acknowledge()
        else throw Exception("Special exception for you!")
    }

}
