package com.countrydelight.executor;

import com.countrydelight.populator.intf.KafkaPopulatorInterface;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service("executorService")
public class ExecutorService {

	@Autowired
	private KafkaPopulatorInterface kafkaPopulator;
}
