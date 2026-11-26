package cl.duoc.backend_skates_app;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import java.math.BigDecimal;
import java.util.List;

import jakarta.validation.Validation;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import cl.duoc.backendskatesapp.service.NonRecoverableMessageException;
import cl.duoc.backendskatesapp.streaming.analytics.AnalyticsEventService;
import cl.duoc.backendskatesapp.streaming.consumer.StreamEventHandler;
import cl.duoc.backendskatesapp.streaming.event.OrderStreamEvent;
import cl.duoc.backendskatesapp.streaming.notifications.NotificationEventService;
import tools.jackson.databind.ObjectMapper;

class StreamEventHandlerTest {
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final AnalyticsEventService analytics = Mockito.mock(AnalyticsEventService.class);
    private final NotificationEventService notifications = Mockito.mock(NotificationEventService.class);
    private final StreamEventHandler handler = new StreamEventHandler(
            objectMapper, Validation.buildDefaultValidatorFactory().getValidator(), analytics, notifications);

    @Test
    void rejectsInvalidJsonAndUnknownEventTypesAsNonRecoverable() {
        assertThrows(NonRecoverableMessageException.class,
                () -> handler.validateAndProcess("{", "skates-analytics-app", "analytics"));
        assertThrows(NonRecoverableMessageException.class,
                () -> handler.validateAndProcess("{\"eventType\":\"UNSUPPORTED\"}", "skates-analytics-app", "analytics"));
        Mockito.verifyNoInteractions(analytics, notifications);
    }

    @Test
    void validatesRequiredEventFieldsBeforeDispatch() {
        assertThrows(NonRecoverableMessageException.class,
                () -> handler.validateAndProcess(
                        "{\"eventId\":null,\"eventType\":\"PAYMENT_COMPLETED\",\"schemaVersion\":1}",
                        "skates-analytics-app", "analytics"));
        verifyNoInteractions(analytics, notifications);
    }

    @Test
    void routesValidOrderEventToRequestedConsumerDomain() {
        OrderStreamEvent event = OrderStreamEvent.create("PAYMENT_COMPLETED", "test", "corr",
                "buy-order", BigDecimal.TEN, "COMPLETED", List.of(), "auth", null);

        handler.validateAndProcess(objectMapper.writeValueAsString(event), "skates-notifications-app", "notifications");

        verify(notifications).process(event, "skates-notifications-app");
        verifyNoInteractions(analytics);
    }
}
