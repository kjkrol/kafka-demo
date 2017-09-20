package kjkrol.kafkademo.consumer

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import javax.validation.constraints.NotNull
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.annotation.EnableKafka
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.listener.AbstractMessageListenerContainer
import org.springframework.kafka.support.serializer.JsonDeserializer
import java.io.Serializable
import javax.validation.Valid
import org.xerial.snappy.buffer.DefaultBufferAllocator.factory
import org.springframework.retry.policy.SimpleRetryPolicy
import org.springframework.retry.support.RetryTemplate



@ConfigurationProperties("app.kafka.consumer")
@Configuration
@EnableKafka
internal class VideoContentConsumerConfiguration(
        @Valid
        @NotNull
        var bootstrapServers: String = "",
        @Valid
        @NotNull
        var groupIdConfig: String = "",
        @Valid
        @NotNull
        var autoOffsetReset: String = "") {

    @Bean
    fun videoContentConsumer(): VideoContentConsumer = VideoContentConsumer()

    @Bean
    fun kafkaListenerContainerFactory(objectMapper: ObjectMapper): ConcurrentKafkaListenerContainerFactory<String, VideoContent> {
        val factory: ConcurrentKafkaListenerContainerFactory<String, VideoContent> = ConcurrentKafkaListenerContainerFactory()
        factory.consumerFactory = consumerFactory(objectMapper)
        factory.setConcurrency(1)
        factory.isBatchListener = false
        factory.containerProperties.pollTimeout = 3000
        factory.containerProperties.ackMode = AbstractMessageListenerContainer.AckMode.MANUAL_IMMEDIATE

        val rt = RetryTemplate()
        val srp = SimpleRetryPolicy()
        srp.maxAttempts = 10
        rt.setRetryPolicy(srp)
        factory.setRetryTemplate(rt)

        return factory
    }

    fun consumerFactory(objectMapper: ObjectMapper): ConsumerFactory<String, VideoContent> = DefaultKafkaConsumerFactory<String, VideoContent>(
            consumerConfig(),
            StringDeserializer(),
            JsonDeserializer(VideoContent::class.java, objectMapper)
    )

    fun consumerConfig(): Map<String, Serializable> = hashMapOf(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to autoOffsetReset,
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to JsonDeserializer::class.java,
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to false,
            ConsumerConfig.GROUP_ID_CONFIG to groupIdConfig
    )

}