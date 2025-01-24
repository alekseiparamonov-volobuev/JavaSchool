package sbp.school.kafka.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import sbp.school.kafka.config.KafkaProducerConfig;
import sbp.school.kafka.config.KafkaProperties;
import sbp.school.kafka.dto.TransactionDto;
import sbp.school.kafka.utils.TransactionGenerator;

@Slf4j
public class ProducerService extends Thread {

    private final KafkaProducer<String, TransactionDto> producer;
    private final String topic;

    public ProducerService() {
        this.topic = KafkaProperties.getTransactionTopic();
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
        log.info("Отправка {} в топик {}", transaction, topic);
        try {
            producer.send(new ProducerRecord<>(topic, transaction.getOperationType().name(), transaction),
                    (recordMetadata, exception) -> {
                        if (exception == null) {
                            log.debug("topic = {}, offset = {}, partition = {}",
                                    topic, recordMetadata.offset(), recordMetadata.partition());
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
}
