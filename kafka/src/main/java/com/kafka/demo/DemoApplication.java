package com.kafka.demo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.servlet.function.ServerResponse;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Consumer;

@SpringBootApplication
public class DemoApplication {

	public static void main(String[] args) {

		SpringApplication.run(DemoApplication.class, args);
	}

	private static void syncProducer() throws Exception{
		String topicName = "topic-one";

		Properties properties = new Properties();
		// initial connection
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1");

		Producer<String, String> producer = new KafkaProducer<>(properties);

		for (int i =0; i < 5; i++) {
			String key = "key-" + i;
			String value = "value-" + i;

			ProducerRecord<String, String> record = new ProducerRecord<>(topicName, key, value);

			Future<RecordMetadata> response = producer.send(record);
			response.get();

			Thread.sleep(1000);
		}

		producer.flush();
		producer.close();
	}

	private static void  asyncProducer() throws Exception {
		String topicName = "topic-one";

		Properties properties = new Properties();
		// initial connection
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		// we have 0, 1 at least one, or all, control trade off
		properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
		// control unique message, send once even failure
		properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
		// default byte arrays
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		// timeout
		properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1");
		// delivery.timeout.ms for aks

		Producer<String, String> producer = new KafkaProducer<>(properties);
		CountDownLatch countDownLatch = new CountDownLatch(5);

		for (int i =0; i < 5; i++) {
			String key = "key-" + i;
			String value = "value-" + i;

			ProducerRecord<String, String> record = new ProducerRecord<>(topicName, key, value);

			Future<RecordMetadata> response = producer.send(record, new Callback() {
				@Override
				public void onCompletion(RecordMetadata recordMetadata, Exception e) {
					if (e != null) {
						System.out.print("error");
					} else {
						System.out.print("ok");
					}

					countDownLatch.countDown();
				}
			});

			Thread.sleep(1000);
		}
		countDownLatch.await();
		producer.flush();
		producer.close();
	}

	private static void consumer() {
		String defaultTopicName = "test-topic";
		String defaultGroupName = "test-group";

		String topicName = System.getenv("TOPIC_NAME") != null ? System.getenv("TOPIC_NAME") : defaultTopicName;
		String groupName = System.getenv("CONSUMER_GROUP_NAME") != null ? System.getenv("CONSUMER_GROUP_NAME") : defaultGroupName;

		Properties properties = new Properties();

		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupName);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		//  the consumer will start consuming from the earliest available offset in the partition
		// latest, or none
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		// fetch.max.wait.ms
		// This configuration specifies the maximum amount of time in milliseconds that the broker should wait for the minimum number of bytes specified by the fetch.min.bytes configuration to become available before responding to a fetch request.
		// A good starting value is usually around 5 seconds.
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

		consumer.subscribe(Collections.singletonList(topicName));

		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			System.out.println("Stop");
			consumer.close();
		}));

		try {
			while(true) {
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
				for(ConsumerRecord<String, String> record : records) {
					System.out.println(record.topic());
					System.out.println(record.partition());
					System.out.println(record.offset());
					System.out.println(record.key());
					System.out.println(record.value());
				}
			}
		} finally {
			consumer.close();
		}
	}

	/*
	* Arguments

    kafka-topics.sh: This is used to manage the topics in our Kafka server.

    --create: This indicates that we are creating a new topic.
        --topic topic: Here, we specify our topic name.

    --bootstrap-server localhost:9092: Here, we specify the server and the hostname and port number on which our Kafka server is up and running.

    --replication-factor: This is used to give the number of replication factors in our topic.

    --partitions: At the end of our command, we define the number of partitions in our Kafka topic.
*/

	private static void kSteamMap() {
		StreamsBuilder builder = new StreamsBuilder();

		KStream<String, String> stream = builder.stream("input");

		stream.map(new KeyValueMapper<String, String, KeyValue<?, ?>>() {
			@Override
			public KeyValue<String, String> apply(String s, String s2) {
				return new KeyValue<>(s.toLowerCase(), s2.toUpperCase());
			}
		});
	}

	public static void kSteam() {
		Properties config = new Properties();
		config.put(StreamsConfig.APPLICATION_ID_CONFIG, "dsl-api");
		config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String());
		config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String());

		StreamsBuilder builder = new StreamsBuilder();
		KStream<String, String> input = builder.stream("input-topic");

		KStream<String, String> filtered = input.filter((k,v) -> k.length() > 5);
		KStream<String, String> upperCased = filtered.mapValues(value -> value.toUpperCase());

		upperCased.peek((k,v) -> System.out.println(k));

		upperCased.to("output-topic");

		Topology topology = builder.build();
		KafkaStreams streams = new KafkaStreams(topology, config);

		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

		streams.start();
	}

	private static void kStreamStateful() {
		Properties config = new Properties();
		config.put(StreamsConfig.APPLICATION_ID_CONFIG, "counts-app");
		config.put(StreamsConfig.APPLICATION_SERVER_CONFIG, "localhost:9090");
		config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String());
		config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String());

		StreamsBuilder builder = new StreamsBuilder();
		KStream<String, String> source = builder.stream("input-topic");

		source.groupByKey()
				.count()
				.toStream()
				.to("output-topic", Produced.with(Serdes.String(), Serdes.Long()));

		KafkaStreams app = new KafkaStreams(builder.build(), config);

		app.start();
	}
}
