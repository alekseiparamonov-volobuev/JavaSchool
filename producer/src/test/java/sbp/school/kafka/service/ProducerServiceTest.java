package sbp.school.kafka.service;


import org.junit.jupiter.api.Test;
import sbp.school.kafka.dto.TransactionDto;
import sbp.school.kafka.enums.OperationType;

import java.math.BigDecimal;
import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThatNoException;

class ProducerServiceTest {

    @Test
    void testSendShouldDontThrowException() {
        ProducerService producerService = new ProducerService();
        TransactionDto transaction = new TransactionDto()
                .setOperationType(OperationType.SOCIAL_OBJECTIVES)
                .setAmount(new BigDecimal(231))
                .setAccountNumber("124654234")
                .setOperationTime(LocalDateTime.now());
        assertThatNoException().isThrownBy(() -> producerService.send(transaction));
    }
}
