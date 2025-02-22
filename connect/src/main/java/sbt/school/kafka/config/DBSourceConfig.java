package sbt.school.kafka.config;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class DBSourceConfig extends AbstractConfig {

    public static final String DB_URL = "db.url";
    public static final String DB_USER = "db.user";
    public static final String DB_PASSWORD = "db.password";
    public static final String TABLE_NAME = "table.name";
    public static final String TOPIC = "topic";
    public static final String POLL_INTERVAL = "poll.interval.ms";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(DB_URL, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Database URL")
            .define(DB_USER, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Database user")
            .define(DB_PASSWORD, ConfigDef.Type.PASSWORD, ConfigDef.Importance.HIGH, "Database password")
            .define(TABLE_NAME, ConfigDef.Type.STRING, "transactions", ConfigDef.Importance.HIGH, "Source table name")
            .define(TOPIC, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Output topic")
            .define(POLL_INTERVAL, ConfigDef.Type.LONG, 5000, ConfigDef.Importance.MEDIUM, "Poll interval in ms");

    public DBSourceConfig(Map<?, ?> originals) {
        super(CONFIG_DEF, originals);
    }

    public String getTableName() {
        return getString(TABLE_NAME);
    }

    public String getTopic() {
        return getString(TOPIC);
    }

    public long getPollInterval() {
        return getLong(POLL_INTERVAL);
    }
}
