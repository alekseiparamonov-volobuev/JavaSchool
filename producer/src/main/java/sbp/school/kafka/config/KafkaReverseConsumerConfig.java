package sbp.school.kafka.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Properties;

public class KafkaReverseConsumerConfig {

    public static KafkaConsumer<Long, Long> getKafkaConsumer(String groupId) {
        Properties properties = KafkaProperties.getReverseConsumerProperties();
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        return new KafkaConsumer<>(properties);
    }
}
