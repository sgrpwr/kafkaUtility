package in.countrydelight.sagar01.dtos;

import lombok.Data;

@Data
public class KafkaRequestDto {
	
	private String topicName;
	private String sourceType;
	private String body;
}
