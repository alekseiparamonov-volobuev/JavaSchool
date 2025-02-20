package sbt.school.kafka.mapper;

import sbp.school.kafka.dto.TransactionDto;
import sbt.school.kafka.entity.Transaction;

public class TransactionMapper {

    private TransactionMapper() {
    }

    public static TransactionDto toDto(Transaction entity) {
        return new TransactionDto()
                .setOperationTime(entity.getOperationTime())
                .setAmount(entity.getAmount())
                .setOperationType(entity.getOperationType())
                .setAccountNumber(entity.getAccount());
    }

}
