package io.github.sgrpwr.dtos;

import lombok.Data;

@Data
public class KafkaRequestDto {
	
	private String analyticsType;
	private String sourceType;
	private Object body;
}
