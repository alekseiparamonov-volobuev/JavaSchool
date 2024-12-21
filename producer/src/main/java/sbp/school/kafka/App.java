package sbp.school.kafka;

import lombok.extern.slf4j.Slf4j;
import sbp.school.kafka.dto.TransactionDto;
import sbp.school.kafka.enums.OperationType;
import sbp.school.kafka.service.ProducerService;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Slf4j
public class App {

    public static void main(String[] args) {
        ProducerService producerService = new ProducerService();
        producerService.send(new TransactionDto()
                .setOperationType(OperationType.SOCIAL_OBJECTIVES)
                .setAmount(new BigDecimal(231))
                .setAccountNumber("124654234")
                .setOperationTime(LocalDateTime.now()));
    }
}
