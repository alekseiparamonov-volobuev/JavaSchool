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
import sbp.school.kafka.dto.AckDto;
import sbp.school.kafka.dto.TransactionDto;
import sbp.school.kafka.utils.TransactionGenerator;

import java.time.Duration;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Slf4j
public class ProducerService extends Thread {

    private Timer timer;
    public static final String REVERSE_GROUP_ID = "reverse1";
    private final KafkaProducer<String, TransactionDto> producer;
    private final KafkaConsumer<Long, AckDto> reverseConsumer;
    private final String topic;
    private final String reverseTopic;
    private final Map<Long, AckDto> sentTransactionsCheckSums;
    private final Map<Long, AckDto> acceptedTransactionsCheckSums;

    public ProducerService() {
        this.topic = KafkaProperties.getTransactionTopic();
        this.reverseTopic = KafkaProperties.getReverseTransactionTopic();
        this.producer = KafkaProducerConfig.getKafkaProducer();
        this.reverseConsumer = KafkaReverseConsumerConfig.getKafkaConsumer(REVERSE_GROUP_ID);
        this.sentTransactionsCheckSums = new ConcurrentHashMap<>();
        this.acceptedTransactionsCheckSums = new ConcurrentHashMap<>();
    }

    @Override
    public void run() {
        send();
        consumeAcknowledgments();
    }

    public void send() {
        startTimer();
        send(TransactionGenerator.getTransaction());
    }

    public void send(TransactionDto transaction) {
        log.info("Отправка {} в топик {}", transaction, topic);
        try {
            sentTransactionsCheckSums.put(transaction.getOperationTime().toInstant(ZoneOffset.UTC).toEpochMilli(),
                    new AckDto(transaction.getId(), transaction.hashCode()));
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

    public void consumeAcknowledgments() {
        reverseConsumer.subscribe(List.of(reverseTopic));

        while (true) {
            ConsumerRecords<Long, AckDto> records = reverseConsumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<Long, AckDto> record : records) {
                Long checksumKey = record.key();
                int checksumValue = record.value().getTransactionHash();
                log.info("Полученная контрольная сумма : key = {}, value = {}", checksumKey, checksumValue);
                acceptedTransactionsCheckSums.put(checksumKey, record.value());
            }
            reverseConsumer.commitSync();
        }
    }

    /**
     * Запуск выполнения ChecksumScheduledTask.run() сейчас и каждые 10 секунд
     */
    public void startTimer() {
        timer = new Timer();
        timer.scheduleAtFixedRate(new ChecksumScheduledTask(), 0, 10000);
    }

    /**
     * Таска для контроля контрольных хэш-сумм отправленных транзакций.
     */
    private Map<Long, AckDto> getCheckingEntities(Map<Long, AckDto> transactionsCheckSums) {
        return transactionsCheckSums.entrySet()
                .stream()
                .filter(entry -> (System.currentTimeMillis() - entry.getKey() >= 10000))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    /**
     * @param checkSums <время операции (транзакции) в long, хэш-код транзакции>
     * @return сумма хэш-кодов транзакций, выполненных более, чем 10 секунд назад
     */
    private long calculateChecksum(Collection<AckDto> checkSums) {
        return checkSums.stream()
                .map(AckDto::getTransactionHash)
                .reduce(0, Integer::sum);
    }

    private class ChecksumScheduledTask extends TimerTask {

        /**
         * В случае, если суммы совпадают, в БД (Map sentTransactionsCheckSums, Map acceptedTransactionsCheckSums)
         * удаляются данные о проверенных транзакциях.
         */
        @Override
        public void run() {
            log.info("Запуск проверки контрольных сумм.");
            Map<Long, AckDto> sentTransactions = getCheckingEntities(sentTransactionsCheckSums);
            Map<Long, AckDto> acceptedTransactions = getCheckingEntities(acceptedTransactionsCheckSums);
            long sentChecksum = calculateChecksum(sentTransactions.values());
            long acceptedChecksum = calculateChecksum(acceptedTransactions.values());
            if (sentChecksum == acceptedChecksum) {
                log.info("Контрольные суммы проверены - чистим БД!");
                for (Long key : sentTransactions.keySet()) {
                    sentTransactionsCheckSums.remove(key);
                }
                for (Long key : acceptedTransactions.keySet()) {
                    acceptedTransactionsCheckSums.remove(key);
                }
            } else {
                log.info("Проверка контрольных сумм не пройдена. Повторно отправляем сообщения.");
                for (AckDto ack : sentTransactions.values()) {
                    TransactionDto transactionDtoToResend = TransactionGenerator.TRANSACTION_DATABASE.get(ack.getTransactionId());
                    send(transactionDtoToResend);
                }
            }
        }
    }
}


