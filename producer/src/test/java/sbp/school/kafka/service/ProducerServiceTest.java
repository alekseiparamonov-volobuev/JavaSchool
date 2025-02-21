package sbp.school.kafka.service;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import sbp.school.kafka.dto.TransactionDto;
import sbp.school.kafka.utils.TransactionGenerator;
import sbp.school.kafka.utils.TransactionPartitioner;
import sbp.school.kafka.utils.TransactionSerializer;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ProducerServiceTest {

    private static final String MEGATRANSACTION = "megatransaction";
    private static final String YYYY_MM_DD_HH_MM_SS = "yyyy-MM-dd HH:mm:ss";

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    static {
        OBJECT_MAPPER.registerModule(new JavaTimeModule());
        OBJECT_MAPPER.setDateFormat(new SimpleDateFormat(YYYY_MM_DD_HH_MM_SS));
    }

    private MockProducer<String, TransactionDto> mockProducer;
    private ProducerService producerService;
    @Mock
    private TransactionSerializer transactionSerializer;

    @BeforeEach
    void setUp() {
        mockProducer = new MockProducer<>(
                true,
                new StringSerializer(),
                transactionSerializer
        );
        producerService = new ProducerService();
        producerService.setKafkaProducer(mockProducer);
    }

    @Test
    void shouldSendTransactionToKafka() throws JsonProcessingException {
        TransactionDto transactionDto = TransactionGenerator.getTransaction();

        when(transactionSerializer.serialize(MEGATRANSACTION, transactionDto))
                .thenReturn(OBJECT_MAPPER.writeValueAsBytes(transactionDto));

        producerService.send(transactionDto);
        producerService.send(TransactionGenerator.getTransaction());

        ProducerRecord<String, TransactionDto> record = mockProducer.history().get(0);

        assertThat(record.topic()).isEqualTo(MEGATRANSACTION);
        assertThat(record.key()).isEqualTo(transactionDto.getOperationType().name());
        assertThat(record.value()).isEqualTo(transactionDto);
        assertThat(mockProducer.history().size()).isEqualTo(2);
    }

    @Test
    void shouldSendToCorrectPartition() throws JsonProcessingException, ExecutionException, InterruptedException {
        PartitionInfo partitionInfo0 = new PartitionInfo(MEGATRANSACTION, 0, null, null, null);
        PartitionInfo partitionInfo1 = new PartitionInfo(MEGATRANSACTION, 1, null, null, null);
        List<PartitionInfo> partitions = List.of(partitionInfo0, partitionInfo1);

        Cluster cluster = new Cluster("testCluster",
                new ArrayList<>(),
                partitions,
                Collections.emptySet(),
                Collections.emptySet());

        MockProducer<String, TransactionDto> mockProducerLocal = new MockProducer<>(cluster, true,
                new TransactionPartitioner(), new StringSerializer(), transactionSerializer);

        TransactionDto transactionDto = TransactionGenerator.getTransaction();
        when(transactionSerializer.serialize(MEGATRANSACTION, transactionDto))
                .thenReturn(OBJECT_MAPPER.writeValueAsBytes(transactionDto));
        producerService.setKafkaProducer(mockProducerLocal);

        producerService.send(transactionDto);

        ProducerRecord<String, TransactionDto> record = mockProducer.history().get(0);

        assertThat(record.partition()).isEqualTo(transactionDto.getOperationType().ordinal());
        assertThat(mockProducer.history().size()).isEqualTo(1);
    }

    @Test
    void shouldHandleProducerErrors() {
        mockProducer.close();
        TransactionDto tx = TransactionGenerator.getTransaction();

        assertThrows(Exception.class, () ->
                producerService.send(tx));
    }
}
