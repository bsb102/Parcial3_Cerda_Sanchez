package cl.duoc.backendskatesapp.streaming.event;

import java.time.Instant;
import java.util.UUID;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

/**
 * El partitionKey usa skateId para conservar el orden de cambios de stock de cada producto.
 */
public record InventoryStreamEvent(
        @NotNull UUID eventId,
        @NotBlank String eventType,
        @NotNull Integer schemaVersion,
        @NotNull Instant occurredAt,
        @NotBlank String source,
        @NotBlank String correlationId,
        @NotBlank String partitionKey,
        @NotNull Long skateId,
        @NotNull Integer stockBefore,
        @NotNull Integer stockAfter,
        @NotBlank String reason) implements StreamEvent {

    public static InventoryStreamEvent create(
            String source, String correlationId, Long skateId,
            Integer stockBefore, Integer stockAfter, String reason) {
        return new InventoryStreamEvent(
                UUID.randomUUID(), "INVENTORY_UPDATED", 1, Instant.now(), source,
                correlationId, skateId.toString(), skateId, stockBefore, stockAfter, reason);
    }
}
