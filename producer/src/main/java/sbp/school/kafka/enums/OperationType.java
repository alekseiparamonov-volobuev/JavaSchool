package sbp.school.kafka.enums;

import lombok.Getter;

public enum OperationType {
    LENDING_OF_FUNDS("Предоставление денежных средств взаймы"),
    CLEARING_OF_CHEQUES("Обналичивание чеков"),
    REMITTANCE_OF_FUNDS("Перевод денежных средств"),
    BILL_PAYMENT_SERVICES("Услуги по оплате счетов"),
    INVESTMENT_BANKING("Инвестиционный банкинг"),
    SOCIAL_OBJECTIVES("Социальные цели");

    @Getter
    private final String meaning;

    OperationType(String meaning) {
        this.meaning = meaning;
    }

}
