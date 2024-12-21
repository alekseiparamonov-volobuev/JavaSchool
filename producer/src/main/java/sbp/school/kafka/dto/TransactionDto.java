package sbp.school.kafka.dto;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;
import sbp.school.kafka.enums.OperationType;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Getter
@Setter
@ToString
@Accessors(chain = true)
public class TransactionDto {

    private OperationType operationType;
    private BigDecimal amount;
    private String accountNumber;
    private LocalDateTime operationTime;

}
