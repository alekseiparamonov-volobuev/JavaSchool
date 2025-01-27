package sbp.school.kafka.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import sbp.school.kafka.config.KafkaProducerConfig;
import sbp.school.kafka.config.KafkaProperties;
import sbp.school.kafka.config.KafkaReverseConsumerConfig;
import sbp.school.kafka.dto.TransactionDto;
import sbp.school.kafka.utils.TransactionGenerator;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class ProducerService extends Thread {

    private final KafkaProducer<String, TransactionDto> producer;
    private final KafkaConsumer<Long, Long> reverseConsumer;
    private final String topic;
    private final String responseTopic;
    private final Map<Long, TransactionDto> sentTransactions;

    public ProducerService() {
        this.topic = KafkaProperties.getTransactionTopic();
        this.responseTopic = KafkaProperties.getReverseTransactionTopic();
        this.producer = KafkaProducerConfig.getKafkaProducer();
        this.reverseConsumer = KafkaReverseConsumerConfig.getKafkaConsumer("reserve1");
        this.sentTransactions = new ConcurrentHashMap<>();
    }

    @Override
    public void run() {
        send();
    }

    public void send() {
        send(TransactionGenerator.getTransaction());
    }

    public void send(TransactionDto transaction) {
        log.info("Отправка {} в топик {}", transaction, topic);
        try {
            sentTransactions.put(transaction.getId(), transaction);
            producer.send(new ProducerRecord<>(topic, transaction.getOperationType().name(), transaction),
                    (recordMetadata, exception) -> {
                        if (exception == null) {
                            log.debug("topic = {}, offset = {}, partition = {}",
                                    topic, recordMetadata.offset(), recordMetadata.partition());
                            sentTransactions.put(System.currentTimeMillis(), transaction);
                        } else {
                            log.error("{}. offset = {}, partition = {}",
                                    exception.getMessage(), recordMetadata.offset(), recordMetadata.partition());
                        }
                    });
        } catch (Throwable ex) {
            log.error("Ошибка при отправке {} в {}.", transaction, topic, ex);
            producer.flush();
        }
    }

    public void handleAcknowledgment(long checksum) {
        long calculatedChecksum = calculateChecksum(sentTransactions);

        if (calculatedChecksum == checksum) {
            log.info("Подтверждение контрольной суммы : {}", checksum);
            sentTransactions.clear();
        } else {
            log.info("Контрольная сумма расходится! Ожидаемая: {}, полученная: {}", checksum, calculatedChecksum);
            for (Map.Entry<Long, TransactionDto> transaction : sentTransactions.entrySet()) {
                send(transaction.getValue());
            }
        }
    }

    public void consumeAcknowledgments() {
        reverseConsumer.subscribe(List.of(responseTopic));

        while (true) {
            ConsumerRecords<Long, Long> records = reverseConsumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<Long, Long> record : records) {
                long checksum = record.value();
                log.info("Полученная контрольная сумма : {}", checksum);
                handleAcknowledgment(checksum);
            }
            reverseConsumer.commitSync();
        }
    }

    private long calculateChecksum(Map<Long, TransactionDto> transactions) {
        return transactions.keySet()
                .stream()
                .filter(key -> System.currentTimeMillis() - key >= 10000)
                .reduce(0L, Long::sum);
    }
}
