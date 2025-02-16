package sbp.school.kafka.utils;

import sbp.school.kafka.dto.TransactionDto;
import sbp.school.kafka.enums.OperationType;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class TransactionGenerator {

    public static final Map<Long, TransactionDto> TRANSACTION_DATABASE = new HashMap<>();

    public static TransactionDto getTransaction() {
        TransactionDto transactionDto = new TransactionDto()
                .setOperationType(Arrays.stream(OperationType.values())
                        .skip(getRandomNumberInRange(0, OperationType.values().length))
                        .findFirst()
                        .orElse(OperationType.CLEARING_OF_CHEQUES))
                .setAmount(new BigDecimal(getRandomNumberInRange(1000, 100500)))
                .setAccountNumber(String.valueOf(getRandomNumberInRange(1000000000, 99999999999999L)))
                .setOperationTime(LocalDateTime.now());
        TRANSACTION_DATABASE.put(transactionDto.getId(), transactionDto);
        return transactionDto;
    }

    private static long getRandomNumberInRange(long min, long max) {
        return (long) (Math.random() * (max - min) + min);
    }
}
