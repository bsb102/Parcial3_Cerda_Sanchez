package cl.duoc.backendskatesapp.streaming.consumer;

import java.util.Map;

import org.springframework.stereotype.Component;

import cl.duoc.backendskatesapp.service.NonRecoverableMessageException;
import cl.duoc.backendskatesapp.streaming.analytics.AnalyticsEventService;
import cl.duoc.backendskatesapp.streaming.event.InventoryStreamEvent;
import cl.duoc.backendskatesapp.streaming.event.OrderStreamEvent;
import cl.duoc.backendskatesapp.streaming.event.StreamEvent;
import cl.duoc.backendskatesapp.streaming.notifications.NotificationEventService;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validator;
import tools.jackson.core.JacksonException;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.ObjectMapper;

@Component
public class StreamEventHandler {
    private final ObjectMapper objectMapper;
    private final Validator validator;
    private final AnalyticsEventService analyticsService;
    private final NotificationEventService notificationService;

    public StreamEventHandler(ObjectMapper objectMapper, Validator validator,
            AnalyticsEventService analyticsService, NotificationEventService notificationService) {
        this.objectMapper = objectMapper;
        this.validator = validator;
        this.analyticsService = analyticsService;
        this.notificationService = notificationService;
    }

    public StreamEvent validateAndProcess(String json, String applicationName, String consumerType) {
        final Map<String, Object> metadata;
        try {
            metadata = objectMapper.readValue(json, new TypeReference<>() { });
        } catch (JacksonException exception) {
            throw new NonRecoverableMessageException("JSON del evento no válido");
        }
        Object eventTypeValue = metadata.get("eventType");
        if (!(eventTypeValue instanceof String eventType) || eventType.isBlank()) {
            throw new NonRecoverableMessageException("Falta eventType en el evento");
        }
        final StreamEvent event;
        try {
            event = switch (eventType) {
                case "PAYMENT_COMPLETED", "PAYMENT_REJECTED" ->
                        objectMapper.readValue(json, OrderStreamEvent.class);
                case "INVENTORY_UPDATED" -> objectMapper.readValue(json, InventoryStreamEvent.class);
                default -> throw new NonRecoverableMessageException("Tipo de evento desconocido: " + eventType);
            };
        } catch (JacksonException exception) {
            throw new NonRecoverableMessageException("Estructura de evento no válida: " + eventType);
        }
        var violations = validator.validate(event);
        if (!violations.isEmpty()) {
            String details = violations.stream()
                    .map(ConstraintViolation::getPropertyPath)
                    .map(Object::toString)
                    .sorted()
                    .reduce((left, right) -> left + ", " + right)
                    .orElse("campos");
            throw new NonRecoverableMessageException("Campos obligatorios no válidos: " + details);
        }
        if ("analytics".equals(consumerType)) {
            analyticsService.process(event, applicationName);
        } else {
            notificationService.process(event, applicationName);
        }
        return event;
    }
}
