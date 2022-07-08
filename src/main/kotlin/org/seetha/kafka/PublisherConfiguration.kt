package org.seetha.kafka

import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.stereotype.Component
import reactor.kafka.sender.KafkaSender
import reactor.kafka.sender.SenderOptions
import java.util.*

@Component
@ConfigurationProperties("kafka")
class PublisherConfiguration {
    lateinit var kafkaBootstrapServers: String
    lateinit var clientId: String
    lateinit var publisherTopic: String
    lateinit var acks: String

    var maxRequestSize: Int = 0
    var retries: Int = 0
    var lingerMs: Int = 0
    var reconnectBackoffMs: Int = 0
    var retryBackoffMs: Int = 0
    var maxInflightPublishes: Int = 256
    var batchSize: Int = 16384

    // this is default, in general should be much less, but > replica.lag.time.max.ms
    var requestTimeoutMs: Int = 30 * 1000

    private fun getSenderOptions(): SenderOptions<String, ByteArray> {
        val properties = Properties()
        properties[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = kafkaBootstrapServers
        properties[ProducerConfig.CLIENT_ID_CONFIG] = clientId
        properties[ProducerConfig.ACKS_CONFIG] = acks
        properties[ProducerConfig.RETRIES_CONFIG] = retries
        properties[ProducerConfig.LINGER_MS_CONFIG] = lingerMs
        properties[ProducerConfig.MAX_REQUEST_SIZE_CONFIG] = maxRequestSize
        properties[ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG] = reconnectBackoffMs
        properties[ProducerConfig.RETRY_BACKOFF_MS_CONFIG] = retryBackoffMs
        properties[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
        properties[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = ByteArraySerializer::class.java
        properties[ProducerConfig.BATCH_SIZE_CONFIG] = batchSize
        properties[ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG] = requestTimeoutMs

        log.info("Kafka publisher properties configured : {}", properties)

        return SenderOptions.create(properties)
    }

    @Bean
    fun kafkaSender(publisherConfiguration: PublisherConfiguration): KafkaSender<String, ByteArray> {
        return KafkaSender.create(publisherConfiguration.getSenderOptions())
    }

    companion object {
        private val log = LoggerFactory.getLogger(PublisherConfiguration::class.java)
    }
}


