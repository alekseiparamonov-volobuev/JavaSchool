package sbt.school.kafka.connector;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import sbp.school.kafka.dto.TransactionDto;
import sbt.school.kafka.config.FileSinkConfig;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Map;

@Slf4j
public class FileSinkTask extends SinkTask {

    private BufferedWriter writer;
    private String filePath;

    @Override
    public void start(Map<String, String> props) {
        FileSinkConfig config = new FileSinkConfig(props);
        this.filePath = config.getFilePath();

        try {
            writer = new BufferedWriter(new OutputStreamWriter(
                    new FileOutputStream(filePath, true), StandardCharsets.UTF_8));

            log.info("Инициализация BufferedWriter для файла: {}", filePath);
        } catch (IOException e) {
            throw new RuntimeException("Исключение при инициализации BufferedWriter", e);
        }
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        try {
            for (SinkRecord record : records) {
                TransactionDto tx = (TransactionDto) record.value();
                writer.write(txToCsv(tx));
                writer.newLine();
            }
            writer.flush();
            log.debug("Записали {} транзакций в файл.", records.size());
        } catch (IOException e) {
            log.error("Ошибка при записи в файл: {}", e.getMessage());
        }
    }

    private String txToCsv(TransactionDto tx) {
        return String.format("%s,%s,%s,%s",
                tx.getAmount(),
                tx.getAccountNumber(),
                tx.getOperationTime(),
                tx.getOperationType());
    }

    @Override
    public void stop() {
        try {
            if (writer != null) {
                writer.close();
            }
        } catch (IOException e) {
            log.error("Исключение при закрытии BufferedWriter: {}", e.getMessage());
        }
    }

    @Override
    public String version() {
        return "1.0";
    }
}
