package org.seetha

import org.apache.kafka.clients.producer.ProducerRecord
import org.seetha.kafka.PublisherConfiguration
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import org.springframework.stereotype.Service
import reactor.core.Disposables
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.kafka.receiver.KafkaReceiver
import reactor.kafka.receiver.ReceiverRecord
import reactor.kafka.sender.KafkaSender
import reactor.kafka.sender.SenderRecord
import reactor.kafka.sender.SenderResult
import java.util.*
import javax.annotation.PostConstruct
import javax.annotation.PreDestroy

@Service
class Workflow(
    private val kafkaReceiver: KafkaReceiver<String, ByteArray>,
    private val kafkaSender: KafkaSender<String, ByteArray>,
    private val publisherConfiguration: PublisherConfiguration
) {

    private val disposables = Disposables.composite()

    @PostConstruct
    fun connect() {
        disposables.add(
            receive()
                .subscribe(
                    { log.info("Ended subscription to Kafka Receiver") },
                    { err -> log.error("Error in Kafka Receiver flow", err) }
                )
        )
    }

    @PreDestroy
    fun disconnect() {
        this.disposables.dispose()
    }

    fun receive(): Flux<SenderResult<UUID>> {
        return kafkaReceiver.receive()
            .doOnNext {  log.info("Receiving record with key : ${it.key()}") }
            .flatMap {
                processRecord(it)
            }
    }

    fun processRecord(record: ReceiverRecord<String, ByteArray>): Flux<SenderResult<UUID>> {
        var senderRecord = SenderRecord.create(
            ProducerRecord(
                publisherConfiguration.publisherTopic,
                null,
                record.key(),
                record.value(),
                null
            ),
           UUID.randomUUID()
        )
        return Mono.just(senderRecord)
            .let {
                log.info("Sending message with key: ${record.key()}")
                kafkaSender.send(it)
            }
    }

    companion object {
        private val log = LoggerFactory.getLogger(Workflow::class.java)
    }

}