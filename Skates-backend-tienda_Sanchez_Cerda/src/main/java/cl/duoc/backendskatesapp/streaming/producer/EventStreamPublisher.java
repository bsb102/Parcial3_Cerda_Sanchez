package cl.duoc.backendskatesapp.streaming.producer;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;

import cl.duoc.backendskatesapp.config.KinesisProperties;
import cl.duoc.backendskatesapp.streaming.event.StreamEvent;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResultEntry;

@Component
@ConditionalOnProperty(name = "app.kinesis.enabled", havingValue = "true")
public class EventStreamPublisher {

    private static final int MAX_RECORDS_PER_REQUEST = 500;
    private static final Logger logger = LoggerFactory.getLogger(EventStreamPublisher.class);
    private final KinesisClient client;
    private final ObjectMapper objectMapper;
    private final KinesisProperties properties;

    public EventStreamPublisher(KinesisClient client, ObjectMapper objectMapper, KinesisProperties properties) {
        this.client = client;
        this.objectMapper = objectMapper;
        this.properties = properties;
    }

    public void publish(StreamEvent event, String streamName) {
        try {
            var response = client.putRecord(PutRecordRequest.builder()
                    .streamName(streamName)
                    .partitionKey(event.partitionKey())
                    .data(software.amazon.awssdk.core.SdkBytes.fromUtf8String(objectMapper.writeValueAsString(event)))
                    .build());
            logger.info("Evento Kinesis publicado eventId={} stream={} shardId={} sequenceNumber={}",
                    event.eventId(), streamName, response.shardId(), response.sequenceNumber());
        } catch (JacksonException exception) {
            throw new IllegalArgumentException("No se pudo serializar el evento " + event.eventId(), exception);
        }
    }

    public void publishRawRecord(String partitionKey, String json, String streamName) {
        var response = client.putRecord(PutRecordRequest.builder()
                .streamName(streamName)
                .partitionKey(partitionKey)
                .data(software.amazon.awssdk.core.SdkBytes.fromUtf8String(json))
                .build());
        logger.info("Registro de errores publicado en Kinesis stream={} shardId={} sequenceNumber={}",
                streamName, response.shardId(), response.sequenceNumber());
    }

    public void publishBatch(List<? extends StreamEvent> events, String streamName) {
        for (int start = 0; start < events.size(); start += MAX_RECORDS_PER_REQUEST) {
            int end = Math.min(start + MAX_RECORDS_PER_REQUEST, events.size());
            publishBatchChunk(events.subList(start, end), streamName);
        }
    }

    private void publishBatchChunk(List<? extends StreamEvent> events, String streamName) {
        List<PutRecordsRequestEntry> pending = toEntries(events);
        List<? extends StreamEvent> pendingEvents = new ArrayList<>(events);
        for (int attempt = 0; !pending.isEmpty() && attempt <= properties.getConsumers().getMaxRetries(); attempt++) {
            var response = client.putRecords(PutRecordsRequest.builder()
                    .streamName(streamName)
                    .records(pending)
                    .build());
            int failedCount = response.failedRecordCount() == null ? 0 : response.failedRecordCount();
            if (failedCount == 0) {
                logSuccessfulBatch(events, streamName, response.records());
                return;
            }
            List<PutRecordsRequestEntry> failed = new ArrayList<>();
            List<StreamEvent> failedEvents = new ArrayList<>();
            for (int index = 0; index < response.records().size(); index++) {
                PutRecordsResultEntry result = response.records().get(index);
                if (result.errorCode() == null) {
                    logger.info("Evento Kinesis publicado eventId={} stream={} shardId={} sequenceNumber={}",
                            pendingEvents.get(index).eventId(), streamName, result.shardId(), result.sequenceNumber());
                } else {
                    failed.add(pending.get(index));
                    failedEvents.add(pendingEvents.get(index));
                    logger.warn("Falló evento Kinesis eventId={} stream={} causa={} intento={}",
                            pendingEvents.get(index).eventId(), streamName, result.errorMessage(), attempt + 1);
                }
            }
            if (failed.size() != failedCount) {
                throw new IllegalStateException("Kinesis reportó " + failedCount
                        + " registros fallidos, pero la respuesta contiene " + failed.size());
            }
            pending = failed;
            pendingEvents = failedEvents;
            if (!pending.isEmpty() && attempt < properties.getConsumers().getMaxRetries()) {
                sleep(backoff(attempt));
            }
            if (!pending.isEmpty() && attempt == properties.getConsumers().getMaxRetries()) {
                throw new IllegalStateException("Se agotaron los reintentos para " + failedCount
                        + " registros del stream " + streamName);
            }
        }
    }

    private List<PutRecordsRequestEntry> toEntries(List<? extends StreamEvent> events) {
        return events.stream().map(event -> {
            try {
                return PutRecordsRequestEntry.builder()
                        .partitionKey(event.partitionKey())
                        .data(software.amazon.awssdk.core.SdkBytes.fromUtf8String(objectMapper.writeValueAsString(event)))
                        .build();
            } catch (JacksonException exception) {
                throw new IllegalArgumentException("No se pudo serializar el evento " + event.eventId(), exception);
            }
        }).toList();
    }

    private void logSuccessfulBatch(
            List<? extends StreamEvent> events, String streamName, List<PutRecordsResultEntry> results) {
        for (int index = 0; index < results.size(); index++) {
            PutRecordsResultEntry result = results.get(index);
            logger.info("Evento Kinesis publicado eventId={} stream={} shardId={} sequenceNumber={}",
                    events.get(index).eventId(), streamName, result.shardId(), result.sequenceNumber());
        }
    }

    private long backoff(int attempt) {
        long exponential = (long) (properties.getConsumers().getBackoffInitialMs()
                * Math.pow(properties.getConsumers().getBackoffMultiplier(), attempt));
        return Math.min(exponential, properties.getConsumers().getMaxBackoffMs());
    }

    private void sleep(long durationMs) {
        try {
            Thread.sleep(durationMs);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Se interrumpió la espera para reintentar Kinesis", exception);
        }
    }
}
