package sbp.school.kafka;

import sbp.school.kafka.dto.TransactionDto;
import sbp.school.kafka.enums.OperationType;
import sbp.school.kafka.service.ProducerService;

import java.math.BigDecimal;
import java.time.LocalDateTime;

public class ProducerApp {

    public static void main(String[] args) {
        ProducerService producerService = new ProducerService();
        producerService.send(new TransactionDto()
                .setOperationType(OperationType.SOCIAL_OBJECTIVES)
                .setAmount(new BigDecimal(231))
                .setAccountNumber("124654234")
                .setOperationTime(LocalDateTime.now()));
    }
}
