package sbp.school.kafka.dto;

import java.util.Objects;

public class AckDto {

    private long transactionId;
    private int transactionHash;

    public AckDto() {
    }

    public AckDto(long transactionId, int transactionHash) {
        this.transactionId = transactionId;
        this.transactionHash = transactionHash;
    }

    public long getTransactionId() {
        return transactionId;
    }

    public AckDto setTransactionId(long transactionId) {
        this.transactionId = transactionId;
        return this;
    }

    public int getTransactionHash() {
        return transactionHash;
    }

    public AckDto setTransactionHash(int transactionHash) {
        this.transactionHash = transactionHash;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof AckDto ackDto)) return false;
        return transactionId == ackDto.transactionId && transactionHash == ackDto.transactionHash;
    }

    @Override
    public int hashCode() {
        return Objects.hash(transactionId, transactionHash);
    }

    @Override
    public String toString() {
        return "AckDto{" +
               "transactionId=" + transactionId +
               ", transactionHash=" + transactionHash +
               '}';
    }
}
