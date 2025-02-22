package sbt.school.kafka.connector;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import sbt.school.kafka.config.DBSourceConfig;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Source Connector для чтения транзакций из БД (PostgreSQL)
 */
@Slf4j
public class DBSourceConnector extends SourceConnector {
    private Map<String, String> configProps;

    @Override
    public void start(Map<String, String> props) {
        this.configProps = props;
        log.info("Запуск DBSourceConnector c настройками: {}", props);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return DBSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        return Collections.nCopies(maxTasks, configProps);
    }

    @Override
    public void stop() {
        log.info("Остановка DBSourceConnector");
    }

    @Override
    public ConfigDef config() {
        return DBSourceConfig.CONFIG_DEF;
    }

    @Override
    public String version() {
        return "1.0";
    }
}
