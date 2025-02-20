package sbt.school.kafka.connector;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import sbt.school.kafka.config.DBSourceConfig;
import sbt.school.kafka.entity.Transaction;
import sbt.school.kafka.mapper.TransactionMapper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static sbt.school.kafka.config.HibernateUtil.getSessionFactory;

/**
 * Задача для чтения данных из БД
 */
@Slf4j
public class DBSourceTask extends SourceTask {

    private SessionFactory sessionFactory;
    private Long lastProcessedId = 0L;
    private String tableName;
    private String topic;
    private long pollInterval;

    @Override
    public void start(Map<String, String> props) {
        log.info("Инициализация DBSourceTask");

        DBSourceConfig config = new DBSourceConfig(props);
        this.tableName = config.getTableName();
        this.topic = config.getTopic();
        this.pollInterval = config.getPollInterval();

        sessionFactory = getSessionFactory(config);
        log.info("Подключились к БД. Вычитываем транзакции каждые {} ms", pollInterval);
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        Thread.sleep(pollInterval);

        try (Session session = sessionFactory.openSession()) {
            List<Transaction> transactions = session.createQuery(
                            "FROM Transaction WHERE id > :lastId ORDER BY id", Transaction.class)
                    .setParameter("lastId", lastProcessedId)
                    .getResultList();

            if (transactions.isEmpty()) {
                return null;
            }

            List<SourceRecord> records = new ArrayList<>();
            for (Transaction tx : transactions) {
                records.add(new SourceRecord(
                        sourcePartition(),
                        sourceOffset(tx.getId()),
                        topic,
                        null,
                        TransactionMapper.toDto(tx)
                ));
                lastProcessedId = tx.getId();
            }

            log.info("Получили {} транзакций для отправки в Кафку.", transactions.size());
            return records;
        }
    }

    private Map<String, String> sourcePartition() {
        return Collections.singletonMap("table", tableName);
    }

    private Map<String, Long> sourceOffset(Long id) {
        return Collections.singletonMap("id", id);
    }

    @Override
    public void stop() {
        log.info("Остановка DBSourceTask");
        if (sessionFactory != null) {
            sessionFactory.close();
        }
    }

    @Override
    public String version() {
        return new DBSourceConnector().version();
    }
}
