package cl.duoc.backendskatesapp.streaming.notifications;

import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import cl.duoc.backendskatesapp.model.ProcessedMessage;
import cl.duoc.backendskatesapp.repository.ProcessedMessageRepository;
import cl.duoc.backendskatesapp.streaming.event.OrderStreamEvent;
import cl.duoc.backendskatesapp.streaming.event.StreamEvent;
import tools.jackson.databind.ObjectMapper;

@Service
@Profile({"all", "admin", "stream-consumer"})
public class NotificationEventService {
    private final ProcessedMessageRepository processedRepository;
    private final DerivedNotificationRepository notificationRepository;
    private final ObjectMapper objectMapper;

    public NotificationEventService(ProcessedMessageRepository processedRepository,
            DerivedNotificationRepository notificationRepository, ObjectMapper objectMapper) {
        this.processedRepository = processedRepository;
        this.notificationRepository = notificationRepository;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public void process(StreamEvent event, String applicationName) {
        if (processedRepository.existsByConsumerNameAndEventId(applicationName, event.eventId().toString())) return;
        if (!(event instanceof OrderStreamEvent order)) {
            throw new cl.duoc.backendskatesapp.service.NonRecoverableMessageException(
                    "El consumidor de notificaciones no admite el evento " + event.eventType());
        }
        try {
            String message = "La orden " + order.buyOrder() + " quedó " + order.status();
            notificationRepository.save(new DerivedNotification(
                    order.eventId().toString(), order.eventType(), message, order.occurredAt()));
            processedRepository.save(new ProcessedMessage(applicationName, order.eventId().toString(),
                    order.eventType(), objectMapper.writeValueAsString(order)));
        } catch (RuntimeException exception) {
            throw exception;
        }
    }
}
