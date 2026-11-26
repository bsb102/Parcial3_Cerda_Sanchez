package cl.duoc.backendskatesapp.streaming.event;

import java.time.Instant;
import java.util.UUID;

public interface StreamEvent {
    UUID eventId();
    String eventType();
    Integer schemaVersion();
    Instant occurredAt();
    String source();
    String correlationId();
    String partitionKey();
}
