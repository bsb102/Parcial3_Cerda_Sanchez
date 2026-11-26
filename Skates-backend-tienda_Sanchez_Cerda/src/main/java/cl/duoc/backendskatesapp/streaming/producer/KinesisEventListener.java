package cl.duoc.backendskatesapp.streaming.producer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

import cl.duoc.backendskatesapp.config.KinesisProperties;
import cl.duoc.backendskatesapp.streaming.event.InventoryStreamEvent;
import cl.duoc.backendskatesapp.streaming.event.OrderStreamEvent;
import cl.duoc.backendskatesapp.streaming.event.StreamEvent;

@Component
@Profile({"all", "catalog", "commerce", "admin"})
@ConditionalOnProperty(name = "app.kinesis.enabled", havingValue = "true")
public class KinesisEventListener {
    private static final Logger logger = LoggerFactory.getLogger(KinesisEventListener.class);
    private final EventStreamPublisher publisher;
    private final KinesisProperties properties;

    public KinesisEventListener(EventStreamPublisher publisher, KinesisProperties properties) {
        this.publisher = publisher;
        this.properties = properties;
    }

    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT, fallbackExecution = true)
    public void publishOrder(OrderStreamEvent event) {
        safelyPublish(event, properties.getOrders().getName());
    }

    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT, fallbackExecution = true)
    public void publishInventory(InventoryStreamEvent event) {
        safelyPublish(event, properties.getInventory().getName());
    }

    private void safelyPublish(StreamEvent event, String streamName) {
        try {
            publisher.publish(event, streamName);
        } catch (RuntimeException exception) {
            logger.error("No se pudo publicar evento Kinesis eventId={} stream={}; continúa el flujo principal",
                    event.eventId(), streamName, exception);
        }
    }
}
