package sbp.school.kafka.utils;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import sbp.school.kafka.enums.OperationType;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
public class TransactionPartitioner implements Partitioner {

    private final static Map<String, Integer> KEY_PARTITION = Arrays.stream(OperationType.values())
            .collect(Collectors.toMap(OperationType::name, OperationType::ordinal));
    private final static int REQUIRED_NUMBER_OF_PARTITIONS = KEY_PARTITION.size();

    /**
     * Compute the partition for the given record.
     *
     * @param topic      The topic name
     * @param key        The key to partition on (or null if no key)
     * @param keyBytes   The serialized key to partition on( or null if no key)
     * @param value      The value to partition on or null
     * @param valueBytes The serialized value to partition on or null
     * @param cluster    The current cluster metadata
     */
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        if (keyBytes == null
            || !(key instanceof String)) {
            log.error("Ключ (key) должен быть строкой (String)");
            throw new IllegalArgumentException();
        }
        validateKey(key);

        List<PartitionInfo> partitionInfos = cluster.partitionsForTopic(topic);
        validatePartitionSize(partitionInfos.size());

        return KEY_PARTITION.get(key);
    }

    /**
     * This is called when partitioner is closed.
     */
    @Override
    public void close() {

    }

    /**
     * Configure this class with the given key-value pairs
     *
     * @param configs
     */
    @Override
    public void configure(Map<String, ?> configs) {

    }

    /**
     * @param size количество партиций в топике
     * @throws IllegalArgumentException если количество партиций менее количества операции {@link OperationType}
     */
    private void validatePartitionSize(int size) {
        if (size < REQUIRED_NUMBER_OF_PARTITIONS) {
            String message = String.format("Количество партиций должно быть не менее %d. Текущее значение: %d",
                    REQUIRED_NUMBER_OF_PARTITIONS, size);
            log.error(message);
            throw new IllegalArgumentException(message);
        }
    }

    /**
     * @param key для маппинга на {@link OperationType}
     * @throws IllegalArgumentException {@link Enum#valueOf(Class, String)}
     */
    private void validateKey(Object key) {
        OperationType.valueOf(key.toString());
    }
}
