package kjkrol.kafkademo

import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringSerializer
import org.hibernate.validator.constraints.NotEmpty
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.core.ProducerFactory
import org.springframework.kafka.support.SendResult
import org.springframework.kafka.support.serializer.JsonSerializer
import org.springframework.stereotype.Service
import org.springframework.util.concurrent.ListenableFuture
import org.springframework.validation.annotation.Validated
import javax.validation.Valid

@Validated
@Configuration
@ConfigurationProperties("app.kafka.producer")
internal class ProducerConfiguration {

    @Valid
    @NotEmpty
    var bootstrapServers: String = ""

    @Valid
    @NotEmpty
    var topic: String = ""

    @Bean
    internal fun producerConfigs(): Map<String, Any> = hashMapOf(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to JsonSerializer::class.java
    )

    @Bean
    internal fun producerFactory(): ProducerFactory<String, VideoContent> {
        return DefaultKafkaProducerFactory(producerConfigs())
    }

    @Bean
    internal fun kafkaTemplate(): KafkaTemplate<String, VideoContent> {
        val template : KafkaTemplate<String, VideoContent> = KafkaTemplate(producerFactory())
        template.defaultTopic = topic
        return template
    }

}

@Service
internal class Producer(val template: KafkaTemplate<String, VideoContent>) {

    private companion object {
        val log: Logger = LoggerFactory.getLogger(Producer::class.java)
    }

    internal fun publish(videoContent: VideoContent): ListenableFuture<SendResult<String, VideoContent>> {
        log.info("publishing {}", videoContent)
        return template.sendDefault(videoContent)
    }
}