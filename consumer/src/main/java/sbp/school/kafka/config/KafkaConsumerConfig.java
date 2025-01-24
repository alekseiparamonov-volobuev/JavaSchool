package sbp.school.kafka.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import sbp.school.kafka.dto.TransactionDto;

import java.util.Properties;

public class KafkaConsumerConfig {

    public static KafkaConsumer<String, TransactionDto> getKafkaConsumer(String groupId) {
        Properties properties = KafkaProperties.getConsumerProperties();
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        return new KafkaConsumer<>(properties);
    }
}
