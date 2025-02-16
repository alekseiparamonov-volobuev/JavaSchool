package sbp.school.kafka.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.security.oauthbearer.internals.secured.ValidateException;
import org.apache.kafka.common.serialization.Deserializer;
import sbp.school.kafka.dto.TransactionDto;

import java.io.File;
import java.io.IOException;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
public class TransactionDeserializer implements Deserializer<TransactionDto> {

    private static final String TRANSACTION_JSON = "common/src/main/resources/json/transaction.json";

    @Override
    public TransactionDto deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }

        try {
            ObjectMapper mapper = new ObjectMapper();
            mapper.registerModule(new JavaTimeModule());
            JsonNode jsonSchema = mapper.readTree(new File(TRANSACTION_JSON));
            JsonNode jsonValue = mapper.readTree(data);
            JsonSchemaFactory factory = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V7);
            JsonSchema schema = factory.getSchema(jsonSchema);
            Set<ValidationMessage> validationMessages = schema.validate(jsonValue);
            TransactionDto value = mapper.readValue(data, TransactionDto.class);
            if (!validationMessages.isEmpty()) {
                String validationExceptionMessages = validationMessages.stream()
                        .map(ValidationMessage::getMessage)
                        .collect(Collectors.joining(", "));
                log.error("Исключение при валидации {} : {}", value,
                        validationExceptionMessages);
                throw new ValidateException(validationExceptionMessages);
            }
            return value;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
