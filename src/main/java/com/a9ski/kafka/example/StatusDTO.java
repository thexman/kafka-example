package com.a9ski.kafka.example;

import lombok.Builder;
import lombok.Value;

@Builder(toBuilder = true)
@Value
public class StatusDTO {
	private KafkaStatus status;
	private String key;
	private Exception exception;
	private boolean retriableError;
	
	public static StatusDTO success(String key) {
		return StatusDTO.builder().key(key).status(KafkaStatus.SUCCESS).build();
	}
	
	public static StatusDTO error(String key, Exception exception, boolean retry) {
		return StatusDTO.builder().key(key).status(KafkaStatus.ERROR).exception(exception).retriableError(retry).build();
	}
	
	
}
