package sbp.school.kafka.dto;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;
import sbp.school.kafka.enums.OperationType;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Objects;
import java.util.UUID;

@Getter
@Setter
@ToString
@Accessors(chain = true)
public class TransactionDto {
    private long id;
    private OperationType operationType;
    private BigDecimal amount;
    private String accountNumber;
    private LocalDateTime operationTime;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TransactionDto that)) return false;
        return id == that.id
               && operationType == that.operationType
               && amount.equals(that.amount)
               && accountNumber.equals(that.accountNumber)
               && operationTime.equals(that.operationTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, operationType, amount, accountNumber, operationTime);
    }
}
