package com.a9ski.kafka.example;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;

import lombok.extern.log4j.Log4j2;

@Log4j2
public class Application {

	public static Application INSTANCE;
	
    private final CommandLineArguments arguments;
	

    public static void main(final String[] args) {
        log.info("Starting application...");
        final CommandLineArguments arguments = CommandLineArguments.builder().build();
        final JCommander commander = JCommander.newBuilder()
                .addObject(arguments)
                .programName("myapp")
                .build();

        try {
            commander.parse(args);
            assignInstance(new Application(arguments)).run();
        } catch (final ParameterException ex) {
            log.error("Error parsing arguments: {}", args, ex);
            System.err.println(ex.getMessage());
            commander.usage();
        }
        log.info("Exited application");
    }
    
    public Application(final CommandLineArguments arguments) {
    	this.arguments = arguments;
    }

    protected static Application assignInstance(final Application instance) {
        if (INSTANCE == null) {
            INSTANCE = instance;
        }
        return INSTANCE;
    }

    public void run() {
        log.info("Started application");

        final ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
        	executor.invokeAll(List.of(Executors.callable(this::producer), Executors.callable(this::consumer)), arguments.getTimeout(), TimeUnit.SECONDS);
        	executor.shutdownNow();
		} catch (InterruptedException e) {
			log.info("Executor interrupted. Stopping");
		}

        log.info("Exiting application...");
    }

	private void consumer() {
		Thread.currentThread().setName("consumer");
		final List<DataDTO> data = Collections.synchronizedList(new ArrayList<>());
		long start = System.currentTimeMillis();
		final ExecutorService executor = Executors.newSingleThreadExecutor();
		try (KafkaDataConsumer<DataDTO> c = new KafkaDataConsumer<>(arguments.getBroker(), arguments.getTopic(), DataDTO.class, data::add, false)) {
			executor.execute(c);
			while(data.size() < arguments.getMessageCount()) {
				Thread.sleep(100);
			}
			log.info("Done receiving {} messages. Took {} ms", data.size(), System.currentTimeMillis() - start);
		} catch (InterruptedException e) {
			log.info("Consumer interrupted. Stopping");
		} catch (RuntimeException ex) {
			log.error("Error creating consumer", ex);
		} finally {
			executor.shutdownNow();
		}
		log.info("Total consumed messages: {}", data.size());
		log.info("Messages: {}", data);
	}

	private void producer() {
		try {
			Thread.currentThread().setName("producer");
			final List<DataDTO> data = new ArrayList<>();
			final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
			for(int i = 0; i < arguments.getMessageCount(); i++) {
				data.add(DataDTO.builder().key("k" + i).payload("data" + i).creationDate(sdf.format(new Date())).build());
			}
			try (KafkaDataProducer p = new KafkaDataProducer(arguments.getBroker(), arguments.getTopic())) {
				while(data.size() > 0) {
					final Map<String, StatusDTO> statuses = p.send(data);
					data.removeIf(d -> isNotRetriableError(statuses, d));
				}
			}
		} catch (RuntimeException ex) {
			log.error("Error creating producer", ex);
		}
	}

	private boolean isNotRetriableError(final Map<String, StatusDTO> statuses, DataDTO d) {
		final StatusDTO s = statuses.get(d.getKey());
		return s != null && !(s.getStatus() == KafkaStatus.ERROR && s.isRetriableError());
	}

}
