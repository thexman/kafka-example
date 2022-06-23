package com.a9ski.kafka.example;

import java.util.ArrayList;
import java.util.List;

import com.beust.jcommander.Parameter;
import lombok.Builder;
import lombok.Data;

/**
 * Command line arguments passed to the application.
 */
@Builder(toBuilder = true)
@Data
public class CommandLineArguments {
    @Builder.Default
    @Parameter
    private List<String> parameters = new ArrayList<>();

    @Builder.Default
    @Parameter(names = { "--messages", "-m" }, required = true, description = "number of messages to produce/consume")
    private Integer messageCount = 10;

    @Builder.Default
    @Parameter(names = {"--topic", "-t"}, required = false, description = "kafka topic")
    private String topic = "topic1";
    
    @Builder.Default
    @Parameter(names = {"--broker", "-b"}, required = false, description = "kafka broker")
    private String broker = "127.0.0.1:9092";

    @Builder.Default
    @Parameter(names = {"--timeout", "-T"}, required = false, description = "duration in seconds before terminating the program")
    private long timeout = 42L;
    
    
}
