package sbp.school.kafka.config;

import org.apache.kafka.clients.producer.KafkaProducer;
import sbp.school.kafka.dto.TransactionDto;

import java.util.Properties;

public class KafkaProducerConfig {

    public static KafkaProducer<String, TransactionDto> getMainProducer() {
        Properties properties = KafkaProperties.getMainTransactionProducerProperties();
        return new KafkaProducer<>(properties);
    }
}
