package kjkrol.kafkademo.producer

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringSerializer
import org.hibernate.validator.constraints.NotEmpty
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.core.ProducerFactory
import org.springframework.kafka.support.serializer.JsonSerializer
import java.io.Serializable
import javax.validation.Valid

@Configuration
@ConfigurationProperties("app.kafka.producer")
internal class VideoContentPublisherConfiguration(
        @Valid
        @NotEmpty
        var bootstrapServers: String = "",
        @Valid
        @NotEmpty
        var topic: String = "") {

    @Bean
    internal fun videoContentPublisher(kafkaTemplate: KafkaTemplate<String, VideoContent>): VideoContentPublisher = VideoContentPublisher(kafkaTemplate)

    @Bean
    internal fun kafkaTemplate(objectMapper: ObjectMapper): KafkaTemplate<String, VideoContent> {
        val template: KafkaTemplate<String, VideoContent> = KafkaTemplate(producerFactory(objectMapper))
        template.defaultTopic = topic
        return template
    }

    internal fun producerFactory(objectMapper: ObjectMapper): ProducerFactory<String, VideoContent> = DefaultKafkaProducerFactory(
            configs(),
            StringSerializer(),
            JsonSerializer(objectMapper)
    )

    internal fun configs(): Map<String, Serializable> = hashMapOf(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to JsonSerializer::class.java,
            ProducerConfig.COMPRESSION_TYPE_CONFIG to "gzip"
    )

}