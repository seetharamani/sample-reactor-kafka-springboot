# sample-reactor-kafka-springboot

## Publisher Configuration
Few things to note here are 
- I have configured the publisher with keys of `String` datatype and values of `ByteArray` datatype and so configured the `StringSerializer` and `ByteArraySerializer` in the `SenderOptions` 
- These configs are just for example purposes. Adjust them accordingly for your use case and load
- Publisher topic are constructed only when the ProducerRecord is being constructed. Topic is not configured in the KafkaSender

## Receiver Configuration
Fews things to note here are
- Make sure set the group id for your receiver
- I have configured the publisher with keys of `String` datatype and values of `ByteArray` datatype and so configured the `StringDeserializer` and `ByteArrayDeserializer` in the `ReceiverOptions`
- These configs are just for example purposes. Adjust them accordingly for your use case and load
- Topics to consume from are configured as part of KafkaReceiver

## Starting and stopping the consumer
`kafkaReceiver.receive().subscribe()` starts the consumer and having it in the PostConstruct of the Workflow class makes sure the receiver starts consuming messages once the application context get loaded and the bean creation is completed.

## Integration Test
- With EmbeddedKafka annotation configure the IT test with input and output topics
- Apart from the KafkaReceiver and KafkaSender we created in the `PublisherConfiguration` and `ReceiverConfiguration` classes we need a test kafka sender to publish messages to the input topic to test the Workflow app
- Since the kafka receiver is started right after application start up we need to make sure to disconnect the receiver in `@BeforeEach` so we can have the receiver subscribed in our test case for validation.
- Finally disposing the subscriber after validation as part of the test.
