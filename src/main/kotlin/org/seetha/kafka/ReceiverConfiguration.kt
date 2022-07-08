package org.seetha.kafka

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.stereotype.Component
import reactor.kafka.receiver.KafkaReceiver
import reactor.kafka.receiver.ReceiverOptions
import java.time.Duration
import java.util.*

@Component
@ConfigurationProperties("kafka")
class ReceiverConfiguration {

    lateinit var kafkaBootstrapServers: String
    lateinit var groupId: String
    lateinit var clientId: String
    lateinit var receiverTopic: String
    lateinit var autoOffsetReset: String

    var concurrency: Int = 1
    var maxPollTimeoutMs: Int = 10000
    var backtrackTimeSeconds: Int = 15 * 60

    var retryBackoffMs: Int = 1000
    var maxPollRecords: Int = 250
    var maxPollIntervalMs: Int = 300_000
    var sessionTimeoutMs: Int = 10_000
    var heartbeatIntervalMs: Int = 3000
    var requestTimeoutMs: Int = 30_000
    var autoCommitIntervalMs: Int = 5000
    var commitBatchSize: Int = 0
    var commitIntervalMs: Long = 5000L

    fun getReceiverOptions(): ReceiverOptions<String?, ByteArray> {
        val properties = Properties()
        properties[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = kafkaBootstrapServers
        properties[ConsumerConfig.GROUP_ID_CONFIG] = groupId
        properties[ConsumerConfig.CLIENT_ID_CONFIG] = clientId
        properties[ConsumerConfig.RETRY_BACKOFF_MS_CONFIG] = retryBackoffMs
        properties[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = autoOffsetReset
        properties[ConsumerConfig.MAX_POLL_RECORDS_CONFIG] = maxPollRecords
        properties[ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG] = maxPollIntervalMs
        properties[ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG] = sessionTimeoutMs
        properties[ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG] = heartbeatIntervalMs
        properties[ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG] = requestTimeoutMs
        properties[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
        properties[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = ByteArrayDeserializer::class.java
        properties[ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG] = autoCommitIntervalMs

        log.info("Kafka receiver properties configured : {}", properties)

        return ReceiverOptions.create<String?, ByteArray>(properties)
            .commitInterval(Duration.ofMillis(commitIntervalMs))
            .commitBatchSize(commitBatchSize)
    }

    @Bean
    fun kafkaReceiver(receiverConfiguration: ReceiverConfiguration): KafkaReceiver<String, ByteArray> {
        return KafkaReceiver.create<String, ByteArray>(
            receiverConfiguration.getReceiverOptions().subscription(
                listOf(
                    receiverTopic
                )
            )
        )
    }


    companion object {
        private val log = LoggerFactory.getLogger(ReceiverConfiguration::class.java)
    }
}
