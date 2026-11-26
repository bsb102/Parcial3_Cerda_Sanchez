package cl.duoc.backend_skates_app;

import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import cl.duoc.backendskatesapp.model.ProcessedMessage;
import cl.duoc.backendskatesapp.repository.ProcessedMessageRepository;
import cl.duoc.backendskatesapp.streaming.analytics.AnalyticsEventService;
import cl.duoc.backendskatesapp.streaming.analytics.AnalyticsMetricRepository;
import cl.duoc.backendskatesapp.streaming.event.OrderStreamEvent;
import tools.jackson.databind.ObjectMapper;

class AnalyticsEventServiceTest {
    @Test
    void duplicateEventIsIgnoredForTheSameConsumerApplication() {
        ProcessedMessageRepository processed = Mockito.mock(ProcessedMessageRepository.class);
        AnalyticsMetricRepository metrics = Mockito.mock(AnalyticsMetricRepository.class);
        String eventId = "00000000-0000-0000-0000-000000000001";
        when(processed.existsByConsumerNameAndEventId("analytics-app", eventId)).thenReturn(true);
        AnalyticsEventService service = new AnalyticsEventService(processed, metrics, new ObjectMapper());
        OrderStreamEvent event = new OrderStreamEvent(
                java.util.UUID.fromString(eventId),
                "PAYMENT_COMPLETED", 1, java.time.Instant.parse("2026-01-01T00:00:00Z"),
                "test", "corr", "order-1", "order-1", BigDecimal.TEN, "COMPLETED",
                List.of(), null, null);

        service.process(event, "analytics-app");

        verify(metrics, never()).save(Mockito.any());
        verify(processed, never()).save(Mockito.any(ProcessedMessage.class));
    }
}
