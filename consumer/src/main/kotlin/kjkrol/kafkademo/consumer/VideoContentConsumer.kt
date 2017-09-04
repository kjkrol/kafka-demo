package kjkrol.kafkademo.consumer

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.hibernate.validator.constraints.NotEmpty
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.event.EventListener
import org.springframework.kafka.annotation.EnableKafka
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.event.ListenerContainerIdleEvent
import org.springframework.kafka.support.serializer.JsonDeserializer
import org.springframework.stereotype.Service
import org.springframework.validation.annotation.Validated
import java.io.Serializable
import javax.validation.Valid


@Validated
@ConfigurationProperties("app.kafka.consumer")
@Configuration
@EnableKafka
internal class VideoContentConsumerConfiguration(
        @Valid
        @NotEmpty
        var bootstrapServers: String = "",
        @Valid
        @NotEmpty
        var groupIdConfig: String = "",
        @Valid
        @NotEmpty
        var autoOffsetReset: String = "") {

    @Bean
    fun consumerConfig(): Map<String, Serializable> = hashMapOf(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to autoOffsetReset,
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to JsonDeserializer::class.java,
            ConsumerConfig.GROUP_ID_CONFIG to groupIdConfig
    )

    @Bean
    fun consumerFactory(): ConsumerFactory<String, VideoContent> = DefaultKafkaConsumerFactory<String, VideoContent>(
            consumerConfig(),
            StringDeserializer(),
            JsonDeserializer(VideoContent::class.java)
    )

    @Bean
    fun kafkaListenerContainerFactory(): ConcurrentKafkaListenerContainerFactory<String, VideoContent> {
        val factory: ConcurrentKafkaListenerContainerFactory<String, VideoContent> = ConcurrentKafkaListenerContainerFactory()
        factory.consumerFactory = consumerFactory()
        factory.setConcurrency(3)
        factory.containerProperties.pollTimeout = 3000
        factory.isBatchListener = true
        return factory
    }

}

@Service
class VideoContentConsumer {

    private companion object {
        val log: Logger = LoggerFactory.getLogger(VideoContentConsumer::class.java)
    }

    @KafkaListener(id = "abc", topics = arrayOf("\${app.kafka.consumer.topic}"), containerFactory = "kafkaListenerContainerFactory")
    @Throws(Exception::class)
    internal fun listen(consumerRecords: List<ConsumerRecord<String, VideoContent>>) {
        log.info("consuming {}", consumerRecords)
    }

    @EventListener(condition = "event.listenerId.startsWith('abc-')")
    fun eventHandler(event: ListenerContainerIdleEvent) {
        log.warn("event cached: {}", event.toString())
    }

}
