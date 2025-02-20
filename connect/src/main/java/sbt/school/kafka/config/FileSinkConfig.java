package sbt.school.kafka.config;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class FileSinkConfig extends AbstractConfig {

    public static final String FILE_PATH = "file.path";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(FILE_PATH, ConfigDef.Type.STRING, "/tmp/kafka-output.csv",
                    ConfigDef.Importance.HIGH, "Output file path");

    public FileSinkConfig(Map<?, ?> originals) {
        super(CONFIG_DEF, originals);
    }

    public String getFilePath() {
        return getString(FILE_PATH);
    }
}
