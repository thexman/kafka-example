package com.a9ski.kafka.example;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.StringSerializer;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import lombok.extern.log4j.Log4j2;

@Log4j2
public class KafkaDataProducer implements AutoCloseable {
	private KafkaProducer<String, String> producer;

	private String kafkaTopic;

	private final Gson gson = new GsonBuilder().create();

	public KafkaDataProducer(String broker, String topic) {
		final Properties properties = getKafkaProperties(broker);
		this.kafkaTopic = topic;
		this.producer = new KafkaProducer<>(properties);
	}

	protected Properties getKafkaProperties(String kafkaBroker) {
		final Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBroker);
		properties.setProperty(ProducerConfig.CLIENT_ID_CONFIG, getClass().getSimpleName());
		properties.setProperty("security.protocol", SecurityProtocol.PLAINTEXT.name);
		properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		properties.setProperty(ProducerConfig.CLIENT_DNS_LOOKUP_CONFIG, "use_all_dns_ips");

		properties.put(ProducerConfig.ACKS_CONFIG, "all");
		properties.put(CommonClientConfigs.RETRIES_CONFIG, Integer.MAX_VALUE);
		properties.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 5000);

		properties.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120000); // 2 minutes
		properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 16_384); // 16KB
		properties.put(ProducerConfig.LINGER_MS_CONFIG, 100);
		properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33_554_432); // 32MB

		properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");

		return properties;
	}

	public Map<String, StatusDTO> send(Collection<DataDTO> messages) {
		final Map<String, StatusDTO> statuses = new HashMap<>();

		if (!messages.isEmpty()) {
			log.info("Start sending {} messages", messages.size());
			long start = System.currentTimeMillis();

			try {
				List<CompletableFuture<StatusDTO>> futures = messages.stream().map(this::send)
						.collect(Collectors.toList());

				for (CompletableFuture<StatusDTO> f : futures) {
					try {
						final StatusDTO status = f.get();
						statuses.put(status.getKey(), status);
					} catch (ExecutionException e) {
						log.error("Error completing future", e);
					}
				}
			} catch (final InterruptedException ex) {
				log.error("Interrupted while sending messages to Kafka", ex);
			} finally {
				producer.flush();
			}

			log.info("Done sending {} messages. Took {} ms", messages.size(), System.currentTimeMillis() - start);
		}

		return statuses;
	}

	private CompletableFuture<StatusDTO> send(DataDTO message) {
		CompletableFuture<StatusDTO> future = new CompletableFuture<>();
		try {
			final ProducerRecord<String, String> record = new ProducerRecord<>(kafkaTopic, message.getKey(),
					gson.toJson(message));

			producer.send(record, (metadata, exception) -> {
				if (exception != null) {
					log.error("Unable to send message {}", message.getKey(), exception);
					future.complete(StatusDTO.error(message.getKey(), exception, true));
				} else if (metadata != null) {
					future.complete(StatusDTO.success(message.getKey()));
				} else {
					future.complete(StatusDTO.error(message.getKey(), new IllegalStateException("Empty metadata returned by producer.send()"), true));
				}
			});
		} catch (final RuntimeException ex) {
			log.warn("Error sending messages to Kafka", ex);
			future.complete(StatusDTO.error(message.getKey(), ex, true));
		}

		return future;
	}

	public void close() {
		producer.close();
	}
}
