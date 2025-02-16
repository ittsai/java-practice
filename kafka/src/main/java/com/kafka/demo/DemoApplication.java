package com.kafka.demo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

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
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

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

}
