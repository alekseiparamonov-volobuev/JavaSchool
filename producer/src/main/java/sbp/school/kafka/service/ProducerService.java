package sbp.school.kafka.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import sbp.school.kafka.config.KafkaProducerConfig;
import sbp.school.kafka.config.KafkaProperties;
import sbp.school.kafka.dto.TransactionDto;
import sbp.school.kafka.utils.TransactionGenerator;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

@Slf4j
public class ProducerService extends Thread {

    private final KafkaProducer<String, TransactionDto> producer;
    private final String transactionTopic;

    public ProducerService() {
        this.transactionTopic = KafkaProperties.getTransactionTopic();
        this.producer = KafkaProducerConfig.getKafkaProducer();
    }

    @Override
    public void run() {
        send();
    }

    public void send() {
        send(TransactionGenerator.getTransaction());
    }

    public void send(TransactionDto transaction) {
        log.info("Отправка {} в топик {}", transaction, transactionTopic);
        Future<RecordMetadata> record = producer.send(new ProducerRecord<>(transactionTopic, transaction.getOperationType().name(), transaction),
                (recordMetadata, exception) -> {
                    if (exception != null) {
                        log.error("{}. offset = {}, partition = {}",
                                exception.getMessage(), recordMetadata.offset(), recordMetadata.partition());
                    }
                });
        try {
            log.info("Partition = {}", record.get().partition());
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
        producer.flush();
    }
}
