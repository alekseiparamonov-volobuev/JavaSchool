package sbp.school.kafka.utils;

import sbp.school.kafka.dto.TransactionDto;
import sbp.school.kafka.enums.OperationType;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Arrays;

public class TransactionGenerator {

    public static TransactionDto getTransaction() {
        return new TransactionDto()
                .setOperationType(Arrays.stream(OperationType.values())
                        .skip(getRandomNumberInRange(0, OperationType.values().length))
                        .findFirst()
                        .orElse(OperationType.CLEARING_OF_CHEQUES))
                .setAmount(new BigDecimal(getRandomNumberInRange(1000, 100500)))
                .setAccountNumber(String.valueOf(getRandomNumberInRange(1000000000, 99999999999999L)))
                .setOperationTime(LocalDateTime.now());
    }

    private static long getRandomNumberInRange(long min, long max) {
        return (long) (Math.random() * (max - min) + min);
    }
}
