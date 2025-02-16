package com.kafka.demo;

import org.apache.kafka.clients.producer.*;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

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
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1");

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

}
