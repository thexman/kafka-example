package com.a9ski.kafka.example;

import com.google.gson.annotations.SerializedName;

import lombok.Builder;
import lombok.Data;

@Builder(toBuilder = true)
@Data
public class DataDTO {
	private String key;
	
	private String id;
    private final String namespace = "com.a9ski.kafka.example";
    @SerializedName("$schema")
    private String schema;
    
    private String payload;
    private String creationDate;
}
