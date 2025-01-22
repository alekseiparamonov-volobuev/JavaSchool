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

    private static final int RECORD_TO_COMMIT_COUNT = 150;
    private final String groupId;
    private final Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();

    public ConsumerService(String groupId) {
        this.groupId = groupId;

    }

    public void poll() {
        int counter = 0;
        KafkaConsumer<String, TransactionDto> consumer = KafkaConsumerConfig.getKafkaConsumer(groupId);
        try {
            consumer.subscribe(List.of(KafkaProperties.getTransactionTopic()));
            while (true) {
                ConsumerRecords<String, TransactionDto> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, TransactionDto> record : records) {
                    log.info("topic = {}, partition = {}, offset = {}, groupId = {}, value = {}",
                            record.topic(), record.partition(), record.offset(), consumer.groupMetadata().groupId(), record.value());
                    currentOffsets.put(new TopicPartition(record.topic(), record.partition()),
                            new OffsetAndMetadata(record.offset() + 1));
                    if (counter % RECORD_TO_COMMIT_COUNT == 0) {
                        consumer.commitAsync(currentOffsets, null);
                        counter++;
                    }
                }
                consumer.commitAsync();
            }
        } catch (Exception ex) {
            log.error("Unexpected error or OutOfMemory", ex);
            consumer.commitAsync();
        } finally {
            consumer.close();
        }
    }

    @Override
    public void run() {
        poll();
    }
}
