package sbp.school.kafka.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import sbp.school.kafka.config.KafkaConsumerConfig;
import sbp.school.kafka.config.KafkaProperties;
import sbp.school.kafka.dto.TransactionDto;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class ConsumerService extends Thread {

    private final String groupId;
    private Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();

    public ConsumerService(String groupId) {
        this.groupId = groupId;

    }

    public void poll() {
        int counter = 0;
        try (KafkaConsumer<String, TransactionDto> consumer = KafkaConsumerConfig.getKafkaConsumer(groupId)) {
            consumer.subscribe(List.of(KafkaProperties.getTransactionTopic()));
            while (true) {
                ConsumerRecords<String, TransactionDto> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, TransactionDto> record : records) {
                    log.info("topic = {}, partition = {}, offset = {}, groupId = {}, value = {}",
                            record.topic(), record.partition(), record.offset(), consumer.groupMetadata().groupId(), record.value());
                    currentOffsets.put(new TopicPartition(record.topic(), record.partition()),
                            new OffsetAndMetadata(record.offset() + 1));
                    if (counter % 150 == 0) {
                        consumer.commitAsync(currentOffsets, null);
                        counter++;
                    }
                }
                consumer.commitAsync();
            }
        } catch (Exception ex) {
            log.error("Unexpected error or OutOfMemory", ex);
        }
    }

    @Override
    public void run() {
        poll();
    }
}
