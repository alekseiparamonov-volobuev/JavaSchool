package sbp.school.kafka.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import sbp.school.kafka.utils.PropertiesLoader;

import java.io.IOException;
import java.util.Properties;

@Slf4j
public class KafkaProperties {

    private static final String CONSUMER_PROPERTIES_FILE = "consumer.properties";
    private static final String REVERSE_CONSUMER_PROPERTIES_FILE = "reverse.consumer.properties";
    private static final String PRODUCER_PROPERTIES_FILE = "producer.properties";
    private static final String COMMON_PROPERTIES_FILE = "common.properties";
    private static final String CHECKSUM_PRODUCER_PROPERTIES_FILE = "producer.checksum.properties";

    public static Properties getConsumerProperties() {
        return getConsumerProperties(CONSUMER_PROPERTIES_FILE);
    }

    public static Properties getReverseConsumerProperties() {
        return getConsumerProperties(REVERSE_CONSUMER_PROPERTIES_FILE);
    }

    public static Properties getMainTransactionProducerProperties() {
        try {
            Properties fileProps = PropertiesLoader.loadProperties(PRODUCER_PROPERTIES_FILE);
            Properties appProps = new Properties();
            putProperty(appProps, fileProps, ProducerConfig.BOOTSTRAP_SERVERS_CONFIG);
            putProperty(appProps, fileProps, ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG);
            putProperty(appProps, fileProps, ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG);
            putProperty(appProps, fileProps, ProducerConfig.PARTITIONER_CLASS_CONFIG);
            putProperty(appProps, fileProps, ProducerConfig.ACKS_CONFIG);
            putProperty(appProps, fileProps, ProducerConfig.COMPRESSION_TYPE_CONFIG);
            return appProps;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Properties getChecksumTransactionProducerProperties() {
        try {
            Properties fileProps = PropertiesLoader.loadProperties(CHECKSUM_PRODUCER_PROPERTIES_FILE);
            Properties appProps = new Properties();
            putProperty(appProps, fileProps, ProducerConfig.BOOTSTRAP_SERVERS_CONFIG);
            putProperty(appProps, fileProps, ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG);
            putProperty(appProps, fileProps, ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG);
            putProperty(appProps, fileProps, ProducerConfig.ACKS_CONFIG);
            return appProps;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static String getTransactionTopic() {
        try {
            Properties fileProps = PropertiesLoader.loadProperties(COMMON_PROPERTIES_FILE);
            return fileProps.getProperty("transaction.topic");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static String getReverseTransactionTopic() {
        try {
            Properties fileProps = PropertiesLoader.loadProperties(COMMON_PROPERTIES_FILE);
            return fileProps.getProperty("reverse.topic");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static Properties getConsumerProperties(String propertiesFile) {
        try {
            Properties fileProps = PropertiesLoader.loadProperties(propertiesFile);
            Properties appProps = new Properties();
            putProperty(appProps, fileProps, ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG);
            putProperty(appProps, fileProps, ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG);
            putProperty(appProps, fileProps, ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG);
            return appProps;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void putProperty(Properties appProps, Properties fileProps, String property) {
        String propValue = fileProps.getProperty(property);
        log.info("{} : {}", property, propValue);
        appProps.put(property, propValue);
    }
}
