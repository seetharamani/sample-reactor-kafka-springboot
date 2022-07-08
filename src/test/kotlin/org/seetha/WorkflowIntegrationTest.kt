package org.seetha

import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.*
import org.junit.jupiter.api.extension.ExtendWith
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.test.context.junit.jupiter.SpringExtension
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.kafka.sender.KafkaSender
import reactor.kafka.sender.SenderOptions
import reactor.kafka.sender.SenderRecord
import java.nio.charset.StandardCharsets
import java.util.*
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals
import kotlin.test.assertTrue

@ExtendWith(SpringExtension::class)
@SpringBootTest
@EmbeddedKafka(
    partitions = 1,
    topics = ["input", "output"]
)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
internal class WorkflowIntegrationTest {

    @Value("\${spring.embedded.kafka.brokers}")
    val brokerAddresses: String = ""

    private lateinit var testDataSender: KafkaSender<String, ByteArray>

    @Autowired
    private lateinit var workflow: Workflow

    @BeforeAll
    fun setUpOnce() {
        testDataSender = testDataSender()
    }


    @BeforeEach
    fun setUp() {
        workflow.disconnect()
    }

    /*@AfterEach
    fun teardown() {
        workflow.disconnect()
    }*/

    @Test
    fun `publish test data receive it and publish again using sample receiver and sender`() {
        val key1 = UUID.randomUUID()
        val value1 = UUID.randomUUID()
        val metaData1 = UUID.randomUUID()
        val record1 = SenderRecord.create(
            ProducerRecord(
                "input",
                null,
                key1.toString(),
                value1.toString().toByteArray(StandardCharsets.UTF_8),
                null
            ),
            metaData1
        )
        val key2 = UUID.randomUUID()
        val value2 = UUID.randomUUID()
        val metaData2 = UUID.randomUUID()
        val record2 = SenderRecord.create(
            ProducerRecord(
                "input",
                null,
                key2.toString(),
                value2.toString().toByteArray(StandardCharsets.UTF_8),
                null
            ),
            metaData2
        )
        val key3 = UUID.randomUUID()
        val value3 = UUID.randomUUID()
        val metaData3 = UUID.randomUUID()
        val record3 = SenderRecord.create(
            ProducerRecord(
                "input",
                null,
                key3.toString(),
                value3.toString().toByteArray(StandardCharsets.UTF_8),
                null
            ),
            metaData3
        )


       Flux.just(record1, record2, record3)
            .let {
                log.info("Sending test data : $it")
                testDataSender.send(it)
            }
           .subscribe()


        val count = 10
        val latch = CountDownLatch(count)
        val disposable = workflow
            .receive()
            .subscribe(
                {
                    assertTrue(it.recordMetadata().timestamp() > 0)
                    log.info("Message received : ${it.correlationMetadata()}")

                },
                { log.info("Failed to receive message : $it") }
            )

        latch.await(5, TimeUnit.SECONDS)
        disposable.dispose()

    }

    private fun testDataSender(): KafkaSender<String, ByteArray> {
        return KafkaSender.create(SenderOptions.create(buildPublisherProperties()))
    }

    private fun buildPublisherProperties(): Properties {
        val properties = Properties()
        properties[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = brokerAddresses
        properties[ProducerConfig.CLIENT_ID_CONFIG] = "test-data-sender"
        properties[ProducerConfig.ACKS_CONFIG] = "all"
        properties[ProducerConfig.RETRIES_CONFIG] = 2000
        properties[ProducerConfig.LINGER_MS_CONFIG] = 5
        properties[ProducerConfig.MAX_REQUEST_SIZE_CONFIG] = 1049600
        properties[ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG] = 1000
        properties[ProducerConfig.RETRY_BACKOFF_MS_CONFIG] = 1000
        properties[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
        properties[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = ByteArraySerializer::class.java
        properties[ProducerConfig.BATCH_SIZE_CONFIG] = 10

        return properties
    }

    companion object {
        private val log = LoggerFactory.getLogger(WorkflowIntegrationTest::class.java)
    }

}