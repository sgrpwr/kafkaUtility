package io.github.sgrpwr.executor;

import io.github.sgrpwr.populator.intf.KafkaPopulatorInterface;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service("executorService")
public class ExecutorService {

	@Autowired
	private KafkaPopulatorInterface kafkaPopulator;
}
