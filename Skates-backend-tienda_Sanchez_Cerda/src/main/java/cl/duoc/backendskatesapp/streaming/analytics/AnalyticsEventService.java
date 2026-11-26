package cl.duoc.backendskatesapp.streaming.analytics;

import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import cl.duoc.backendskatesapp.model.ProcessedMessage;
import cl.duoc.backendskatesapp.repository.ProcessedMessageRepository;
import cl.duoc.backendskatesapp.streaming.event.InventoryStreamEvent;
import cl.duoc.backendskatesapp.streaming.event.OrderStreamEvent;
import cl.duoc.backendskatesapp.streaming.event.StreamEvent;
import tools.jackson.databind.ObjectMapper;

@Service
@Profile({"all", "admin", "stream-consumer"})
public class AnalyticsEventService {
    private static final String CONSUMER = "analytics";
    private final ProcessedMessageRepository processedRepository;
    private final AnalyticsMetricRepository metricRepository;
    private final ObjectMapper objectMapper;

    public AnalyticsEventService(ProcessedMessageRepository processedRepository,
            AnalyticsMetricRepository metricRepository, ObjectMapper objectMapper) {
        this.processedRepository = processedRepository;
        this.metricRepository = metricRepository;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public void process(StreamEvent event, String applicationName) {
        if (processedRepository.existsByConsumerNameAndEventId(applicationName, event.eventId().toString())) return;
        try {
            String payload = objectMapper.writeValueAsString(event);
            if (event instanceof OrderStreamEvent order) {
                metricRepository.save(new AnalyticsMetric(order.eventId().toString(), order.eventType(),
                        order.buyOrder(), order.amount(), null, null, order.occurredAt()));
            } else if (event instanceof InventoryStreamEvent inventory) {
                metricRepository.save(new AnalyticsMetric(inventory.eventId().toString(), inventory.eventType(),
                        null, null, inventory.skateId(), inventory.stockAfter(), inventory.occurredAt()));
            }
            processedRepository.save(new ProcessedMessage(applicationName, event.eventId().toString(),
                    event.eventType(), payload));
        } catch (RuntimeException exception) {
            throw exception;
        }
    }
}
