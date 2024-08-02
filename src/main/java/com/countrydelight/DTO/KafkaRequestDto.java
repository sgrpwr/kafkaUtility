package com.countrydelight.DTO;

import lombok.Data;

@Data
public class KafkaRequestDto {
	
	private String analyticsType;
	private String sourceType;
	private Object body;
}
