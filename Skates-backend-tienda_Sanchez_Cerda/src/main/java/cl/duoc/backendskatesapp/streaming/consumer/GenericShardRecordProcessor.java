package cl.duoc.backendskatesapp.streaming.consumer;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import cl.duoc.backendskatesapp.config.KinesisProperties;
import cl.duoc.backendskatesapp.service.NonRecoverableMessageException;
import cl.duoc.backendskatesapp.streaming.event.StreamEvent;
import cl.duoc.backendskatesapp.streaming.producer.EventStreamPublisher;
import tools.jackson.databind.ObjectMapper;
import software.amazon.kinesis.lifecycle.events.InitializationInput;
import software.amazon.kinesis.lifecycle.events.LeaseLostInput;
import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput;
import software.amazon.kinesis.lifecycle.events.ShardEndedInput;
import software.amazon.kinesis.lifecycle.events.ShutdownRequestedInput;
import software.amazon.kinesis.processor.ShardRecordProcessor;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

public class GenericShardRecordProcessor implements ShardRecordProcessor {
    private static final Logger logger = LoggerFactory.getLogger(GenericShardRecordProcessor.class);
    private final String appName;
    private final String consumerType;
    private final KinesisProperties properties;
    private final StreamEventHandler eventHandler;
    private final EventStreamPublisher publisher;
    private final ObjectMapper objectMapper;
    private final List<PendingDeadLetter> pendingDeadLetters = new ArrayList<>();
    private String shardId;

    public GenericShardRecordProcessor(String appName, String consumerType, KinesisProperties properties,
            StreamEventHandler eventHandler, EventStreamPublisher publisher, ObjectMapper objectMapper) {
        this.appName = appName;
        this.consumerType = consumerType;
        this.properties = properties;
        this.eventHandler = eventHandler;
        this.publisher = publisher;
        this.objectMapper = objectMapper;
    }

    @Override
    public void initialize(InitializationInput input) {
        shardId = input.shardId();
        logger.info("Consumidor Kinesis iniciado aplicación={} shardId={}", appName, shardId);
    }

    @Override
    public void processRecords(ProcessRecordsInput input) {
        retryPendingDeadLetters(input);
        for (KinesisClientRecord record : input.records()) {
            String json = StandardCharsets.UTF_8.decode(record.data()).toString();
            int attempts = 0;
            boolean handled = false;
            while (!handled && attempts <= properties.getConsumers().getMaxRetries()) {
                attempts++;
                try {
                    StreamEvent event = eventHandler.validateAndProcess(json, appName, consumerType);
                    MDC.put("eventId", event.eventId().toString());
                    MDC.put("correlationId", event.correlationId());
                    MDC.put("intentos", Integer.toString(attempts));
                    logger.info("Evento Kinesis procesado aplicación={} tipo={} shardId={}",
                            appName, event.eventType(), shardId);
                    handled = true;
                } catch (RuntimeException exception) {
                    String eventId = extractEventId(json);
                    MDC.put("eventId", eventId);
                    MDC.put("correlationId", extractCorrelationId(json));
                    MDC.put("causa", exception.getMessage() == null ? exception.getClass().getSimpleName() : exception.getMessage());
                    MDC.put("intentos", Integer.toString(attempts));
                    if (exception instanceof NonRecoverableMessageException
                            || attempts > properties.getConsumers().getMaxRetries()) {
                        handled = publishToDeadLetter(json, exception, attempts, record);
                        if (!handled) {
                            logger.error("No se pudo enviar evento a DLQ; queda sin checkpoint para reproceso "
                                    + "aplicación={} shardId={} sequenceNumber={}", appName, shardId,
                                    record.sequenceNumber(), exception);
                            pendingDeadLetters.add(new PendingDeadLetter(json, exception, attempts, record));
                            handled = true;
                        }
                    } else {
                        logger.warn("Falla transitoria al procesar Kinesis aplicación={} intento={}",
                                appName, attempts, exception);
                        pause(attempts - 1);
                    }
                } finally {
                    MDC.clear();
                }
            }
            if (handled && pendingDeadLetters.isEmpty()) {
                checkpoint(input, record);
            }
        }
    }

    private void retryPendingDeadLetters(ProcessRecordsInput input) {
        Iterator<PendingDeadLetter> pending = pendingDeadLetters.iterator();
        while (pending.hasNext()) {
            PendingDeadLetter deadLetter = pending.next();
            if (!publishToDeadLetter(deadLetter.original(), deadLetter.cause(),
                    deadLetter.attempts(), deadLetter.record())) {
                return;
            }
            checkpoint(input, deadLetter.record());
            pending.remove();
        }
    }

    private boolean publishToDeadLetter(String original, RuntimeException exception, int attempts,
            KinesisClientRecord record) {
        try {
            String envelope = objectMapper.writeValueAsString(Map.of(
                    "eventoOriginal", original,
                    "causa", exception.getMessage() == null ? exception.getClass().getName() : exception.getMessage(),
                    "intentos", attempts,
                    "streamOrigen", properties.getOrders().getName(),
                    "shardId", shardId == null ? "desconocido" : shardId,
                    "sequenceNumber", record.sequenceNumber(),
                    "aplicacion", appName,
                    "fecha", Instant.now().toString()));
            publisher.publishRawRecord(
                    extractEventId(original), envelope, properties.getDeadLetter().getName());
            logger.warn("Evento enviado a DLQ eventId={} aplicación={} shardId={} intentos={}",
                    extractEventId(original), appName, shardId, attempts);
            return true;
        } catch (RuntimeException publishException) {
            logger.error("Error al enviar evento a DLQ aplicación={} shardId={}", appName, shardId, publishException);
            return false;
        }
    }

    private String extractEventId(String json) {
        try {
            Map<String, Object> metadata = objectMapper.readValue(json, new tools.jackson.core.type.TypeReference<>() { });
            Object value = metadata.get("eventId");
            return value == null ? UUID.randomUUID().toString() : value.toString();
        } catch (RuntimeException exception) {
            return UUID.randomUUID().toString();
        }
    }

    private String extractCorrelationId(String json) {
        try {
            Map<String, Object> metadata = objectMapper.readValue(json, new tools.jackson.core.type.TypeReference<>() { });
            Object value = metadata.get("correlationId");
            return value == null ? "" : value.toString();
        } catch (RuntimeException exception) {
            return "";
        }
    }

    private void pause(int attempt) {
        long backoff = (long) (properties.getConsumers().getBackoffInitialMs()
                * Math.pow(properties.getConsumers().getBackoffMultiplier(), attempt));
        try {
            Thread.sleep(Math.min(backoff, properties.getConsumers().getMaxBackoffMs()));
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
        }
    }

    private void checkpoint(ProcessRecordsInput input, KinesisClientRecord record) {
        try {
            input.checkpointer().checkpoint(record.sequenceNumber());
        } catch (Exception exception) {
            logger.error("No se pudo guardar checkpoint aplicación={} shardId={} sequenceNumber={}",
                    appName, shardId, record.sequenceNumber(), exception);
        }
    }

    private record PendingDeadLetter(
            String original, RuntimeException cause, int attempts, KinesisClientRecord record) { }

    @Override
    public void leaseLost(LeaseLostInput input) {
        logger.info("Se perdió el lease Kinesis aplicación={} shardId={}", appName, shardId);
    }

    @Override
    public void shardEnded(ShardEndedInput input) {
        try {
            input.checkpointer().checkpoint();
        } catch (Exception exception) {
            logger.error("No se pudo completar checkpoint del shard aplicación={} shardId={}", appName, shardId, exception);
        }
    }

    @Override
    public void shutdownRequested(ShutdownRequestedInput input) {
        try {
            input.checkpointer().checkpoint();
        } catch (Exception exception) {
            logger.error("No se pudo guardar checkpoint al detener aplicación={} shardId={}", appName, shardId, exception);
        }
    }
}
