package sbp.school.kafka.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.security.oauthbearer.internals.secured.ValidateException;
import org.apache.kafka.common.serialization.Serializer;
import sbp.school.kafka.dto.TransactionDto;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
public class TransactionSerializer implements Serializer<TransactionDto> {

    private static final String TRANSACTION_JSON = "common/src/main/resources/json/transaction.json";

    /**
     * Convert {@code data} into a byte array.
     *
     * @param topic topic associated with data
     * @param data  typed data
     * @return serialized bytes
     */
    @Override
    public byte[] serialize(String topic, TransactionDto data) {
        if (data == null) {
            throw new NullPointerException("Данные не совсем валидны : [ null ]");
        } else {
            ObjectMapper mapper = new ObjectMapper();
            mapper.registerModule(new JavaTimeModule());
            mapper.setDateFormat(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"));
            try {
                JsonNode jsonSchema = mapper.readTree(getClass().getResourceAsStream(TRANSACTION_JSON));
                String value = mapper.writeValueAsString(data);
                JsonNode jsonValue = mapper.readTree(value);
                JsonSchemaFactory factory = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V7);
                JsonSchema schema = factory.getSchema(jsonSchema);
                Set<ValidationMessage> validationMessages = schema.validate(jsonValue);
                if (!validationMessages.isEmpty()) {
                    String validationExceptionMessages = validationMessages.stream()
                            .map(ValidationMessage::getMessage)
                            .collect(Collectors.joining(", "));
                    log.error("Исключение при валидации {} : {}", value,
                            validationExceptionMessages);
                    throw new ValidateException(validationExceptionMessages);
                }
                return value.getBytes(StandardCharsets.UTF_8);
            } catch (JsonProcessingException e) {
                log.error("Исключение при сериализации {} : {}", data, e.getMessage());
                throw new RuntimeException(e);
            } catch (IOException e) {
                log.error("Исключение при чтении файла {}", TRANSACTION_JSON, e);
                throw new RuntimeException(e);
            }
        }
    }
}
