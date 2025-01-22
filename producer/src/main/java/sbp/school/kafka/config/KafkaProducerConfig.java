package sbp.school.kafka.config;

import org.apache.kafka.clients.producer.KafkaProducer;
import sbp.school.kafka.dto.TransactionDto;

import java.util.Properties;

public class KafkaProducerConfig {

    public static KafkaProducer<String, TransactionDto> getKafkaProducer() {
        Properties properties = KafkaProperties.getKafkaProducerProperties();
        return new KafkaProducer<>(properties);
    }
}
