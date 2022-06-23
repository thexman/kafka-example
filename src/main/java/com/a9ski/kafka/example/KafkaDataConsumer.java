package com.a9ski.kafka.example;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import lombok.extern.log4j.Log4j2;

@Log4j2
public class KafkaDataConsumer<T extends DataDTO> implements Runnable, AutoCloseable {
	private org.apache.kafka.clients.consumer.Consumer<String, String> consumer;

	private final AtomicBoolean exit;
	private AtomicInteger loopThreadCount = new AtomicInteger(1);

	private final java.util.function.Consumer<T> messageConsumer;

	private final String kafkaTopic;

	private final Gson gson = new GsonBuilder().create();
	private Class<T> messageType;

	private final Phaser phaser = new Phaser(1); // 1 register is the thread doing the closing

	public KafkaDataConsumer(String broker, String topic, Class<T> messageType,
			java.util.function.Consumer<T> messageConsumer, boolean autoStart) {
		this.exit = new AtomicBoolean(false);
		this.messageConsumer = messageConsumer;
		this.messageType = messageType;
		this.kafkaTopic = topic;

		final Properties properties = getKafkaProperties(broker);
		this.consumer = new KafkaConsumer<>(properties);
		if (autoStart) {
			run();
		}
	}

	private Properties getKafkaProperties(String kafkaBroker) {
		final Properties properties = new Properties();
		final String groupId = getClass().getSimpleName();

		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBroker);
		properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		properties.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
				CooperativeStickyAssignor.class.getCanonicalName());

		properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

		properties.setProperty(ConsumerConfig.CLIENT_DNS_LOOKUP_CONFIG, "use_all_dns_ips");

		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
		properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

		return properties;
	}

	@Override
	public void run() {
		Thread.currentThread().setName("consumer-thread-" + loopThreadCount.getAndIncrement());
		log.info("Consumer started");
		phaser.register();
		
		log.info("Subscribing to topic {}", kafkaTopic);
		consumer.subscribe(Collections.singleton(kafkaTopic));

		try {
			while (!exit.get()) {
				log.debug("Polling...");
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
				log.debug("Got {} records from topic {}", records.count(), kafkaTopic);
				processMessages(records);
				consumer.commitSync();
			}
		} finally {
			log.debug("Closing consumer");
			consumer.close();
			log.debug("Closed consumer. Exiting consumer thread.");
			phaser.arriveAndDeregister();
			
		}
	}

	private void processMessages(ConsumerRecords<String, String> records) {
		for (ConsumerRecord<String, String> record : records) {
			final T value = gson.fromJson(record.value(), messageType);
			messageConsumer.accept(value);
		}
	}

	public void close() {
		exit.set(true);
		log.info("Waiting for {} consumer threads to exit...", phaser.getRegisteredParties() - 1);

		try {
			int phase = phaser.getPhase();
			phaser.arrive();
			phaser.awaitAdvanceInterruptibly(phase, 5, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			log.error("Interrupted exception occurred while waiting for consumer threads to finish.", e);
		} catch (TimeoutException e) {
			log.error("Timeout while waiting for consumer threads to finish.", e);
		}
	}
}
