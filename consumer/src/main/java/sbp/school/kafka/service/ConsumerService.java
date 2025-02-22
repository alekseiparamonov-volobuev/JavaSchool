package sbp.school.kafka.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import sbp.school.kafka.config.KafkaConsumerConfig;
import sbp.school.kafka.config.KafkaProducerConfig;
import sbp.school.kafka.config.KafkaProperties;
import sbp.school.kafka.dto.AckDto;
import sbp.school.kafka.dto.TransactionDto;

import java.time.Duration;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
public class ConsumerService extends Thread {

    private static final String CHECKSUM_TRANSACTION_TOPIC = KafkaProperties.getReverseTransactionTopic();
    private static final int INITIAL_DELAY = 5;
    private static final int PERIOD = 5;
    private final KafkaConsumer<String, TransactionDto> consumer;
    private final KafkaProducer<Long, AckDto> checksumProducer;
    private static final int RECORD_TO_COMMIT_COUNT = 150;
    private final ScheduledExecutorService executorService;
    private final Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
    private final Map<Long, AckDto> transactionsToSendCheckSums;
    private final Runnable task;

    public ConsumerService(String groupId) {
        this.consumer = KafkaConsumerConfig.getKafkaConsumer(groupId);
        this.checksumProducer = KafkaProducerConfig.getChecksumProducer();
        this.transactionsToSendCheckSums = new ConcurrentHashMap<>();
        this.executorService = Executors.newSingleThreadScheduledExecutor();
        this.task = () -> {
            transactionsToSendCheckSums.forEach(this::sendChecksum);
            transactionsToSendCheckSums.clear();
        };
    }

    public void poll() {
        int counter = 0;
        try {
            consumer.subscribe(List.of(KafkaProperties.getTransactionTopic()));
            while (true) {
                ConsumerRecords<String, TransactionDto> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, TransactionDto> record : records) {
                    TransactionDto transactionDto = record.value();
                    log.info("topic = {}, partition = {}, offset = {}, groupId = {}, value = {}",
                            record.topic(), record.partition(), record.offset(), consumer.groupMetadata().groupId(), transactionDto);
                    transactionsToSendCheckSums.put(transactionDto.getOperationTime().toInstant(ZoneOffset.UTC).toEpochMilli(),
                            new AckDto(transactionDto.getId(), transactionDto.hashCode()));
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
        executorService.scheduleAtFixedRate(task, INITIAL_DELAY, PERIOD, TimeUnit.MINUTES);
        poll();
    }

    private void sendChecksum(Long operationTime, AckDto ackDto) {
        log.info("Отправляю контрольную сумму: key={}, value={}", operationTime, ackDto);
        checksumProducer.send(new ProducerRecord<>(
                CHECKSUM_TRANSACTION_TOPIC,
                operationTime,
                ackDto
        ), this::callbackForChecksum);
    }

    private void callbackForChecksum(RecordMetadata metadata, Exception exception) {
        if (exception != null) {
            log.error("Ошибка при отправке контрольной суммы: {}", exception.getMessage());
        } else {
            log.debug("Контрольная сумма успешно отправлена: offset={}, partition={}",
                    metadata.offset(), metadata.partition());
        }
    }
}
