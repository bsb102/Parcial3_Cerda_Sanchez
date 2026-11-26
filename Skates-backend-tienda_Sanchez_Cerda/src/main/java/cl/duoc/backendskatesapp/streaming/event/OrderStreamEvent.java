package cl.duoc.backendskatesapp.streaming.event;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

/**
 * El partitionKey de órdenes usa buyOrder: Kinesis conserva el orden de los eventos
 * de una compra dentro de un shard, mientras distintas órdenes pueden procesarse en paralelo.
 */
public record OrderStreamEvent(
        @NotNull UUID eventId,
        @NotBlank String eventType,
        @NotNull @Positive Integer schemaVersion,
        @NotNull Instant occurredAt,
        @NotBlank String source,
        @NotBlank String correlationId,
        @NotBlank String partitionKey,
        @NotBlank String buyOrder,
        @NotNull BigDecimal amount,
        @NotBlank String status,
        @NotNull List<@Valid Item> items,
        String authorizationCode,
        String failureReason) implements StreamEvent {

    public static OrderStreamEvent create(
            String eventType,
            String source,
            String correlationId,
            String buyOrder,
            BigDecimal amount,
            String status,
            List<Item> items,
            String authorizationCode,
            String failureReason) {
        return new OrderStreamEvent(
                UUID.randomUUID(), eventType, 1, Instant.now(), source, correlationId,
                buyOrder, buyOrder, amount, status, List.copyOf(items), authorizationCode, failureReason);
    }

    public record Item(@NotNull Long skateId, @NotNull @Positive Integer quantity) { }
}
