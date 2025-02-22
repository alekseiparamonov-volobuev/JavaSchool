package sbt.school.kafka.connector;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import sbt.school.kafka.config.FileSinkConfig;

import java.util.Collections;
import java.util.List;
import java.util.Map;

@Slf4j
public class FileSinkConnector extends SinkConnector {

    private Map<String, String> configProps;

    @Override
    public void start(Map<String, String> props) {
        configProps = props;
        log.info("Запуск FileSinkConnector. Файл-назначение: {}", props.get("file.path"));
    }

    @Override
    public Class<? extends Task> taskClass() {
        return FileSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        return Collections.nCopies(maxTasks, configProps);
    }

    @Override
    public void stop() {
      log.info("Остановка FileSinkConnector");
    }

    @Override
    public ConfigDef config() {
        return FileSinkConfig.CONFIG_DEF;
    }

    @Override
    public String version() {
        return "1.0";
    }
}
