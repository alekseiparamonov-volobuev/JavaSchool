package sbp.school.kafka.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import sbp.school.kafka.config.KafkaProducerConfig;
import sbp.school.kafka.config.KafkaProperties;
import sbp.school.kafka.dto.TransactionDto;

@Slf4j
public class ProducerService {

    private final KafkaProducer<String, TransactionDto> producer;
    private final String transactionTopic;

    public ProducerService() {
        this.transactionTopic = KafkaProperties.getTransactionTopic();
        this.producer = KafkaProducerConfig.getKafkaProducer();
    }

    public void send(TransactionDto transaction) {
        log.info("Отправка {} в топик {}", transaction, transactionTopic);
        producer.send(new ProducerRecord<>(transactionTopic, transaction.getOperationType().name(), transaction),
                (recordMetadata, exception) -> {
                    if (exception != null) {
                        log.error("{}. offset = {}, partition = {}",
                                exception.getMessage(), recordMetadata.offset(), recordMetadata.partition());
                    }
                });
        producer.flush();
    }
}
