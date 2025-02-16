package sbp.school.kafka.config;

import org.apache.kafka.clients.producer.KafkaProducer;
import sbp.school.kafka.dto.AckDto;

import java.util.Properties;

public class KafkaProducerConfig {

    public static KafkaProducer<Long, AckDto> getChecksumProducer() {
        Properties properties = KafkaProperties.getChecksumTransactionProducerProperties();
        return new KafkaProducer<>(properties);
    }
}
