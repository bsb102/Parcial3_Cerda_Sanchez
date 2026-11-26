package cl.duoc.backend_skates_app;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import cl.duoc.backendskatesapp.config.KinesisProperties;
import cl.duoc.backendskatesapp.service.NonRecoverableMessageException;
import cl.duoc.backendskatesapp.streaming.consumer.GenericShardRecordProcessor;
import cl.duoc.backendskatesapp.streaming.consumer.StreamEventHandler;
import cl.duoc.backendskatesapp.streaming.event.OrderStreamEvent;
import cl.duoc.backendskatesapp.streaming.producer.EventStreamPublisher;
import tools.jackson.databind.ObjectMapper;
import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput;
import software.amazon.kinesis.processor.RecordProcessorCheckpointer;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

class GenericShardRecordProcessorTest {
    @Test
    void sendsIrrecoverableRecordToDeadLetterBeforeCheckpointing() throws Exception {
        KinesisProperties properties = new KinesisProperties();
        StreamEventHandler handler = Mockito.mock(StreamEventHandler.class);
        EventStreamPublisher publisher = Mockito.mock(EventStreamPublisher.class);
        RecordProcessorCheckpointer checkpointer = Mockito.mock(RecordProcessorCheckpointer.class);
        when(handler.validateAndProcess(anyString(), anyString(), anyString()))
                .thenThrow(new NonRecoverableMessageException("JSON del evento no válido"));
        GenericShardRecordProcessor processor = new GenericShardRecordProcessor(
                properties.getConsumers().getAnalyticsAppName(), "analytics", properties,
                handler, publisher, new ObjectMapper());
        KinesisClientRecord record = KinesisClientRecord.builder()
                .sequenceNumber("sequence-1")
                .data(ByteBuffer.wrap("{".getBytes(StandardCharsets.UTF_8)))
                .partitionKey("partition")
                .build();
        ProcessRecordsInput input = ProcessRecordsInput.builder()
                .records(List.of(record))
                .checkpointer(checkpointer)
                .build();

        processor.processRecords(input);

        verify(publisher).publishRawRecord(
                Mockito.anyString(), Mockito.argThat(json -> json.contains("\"eventoOriginal\":\"{\"")),
                Mockito.eq(properties.getDeadLetter().getName()));
        verify(checkpointer).checkpoint("sequence-1");
    }

    @Test
    void retriesTransientFailureBeforeCheckpointingSuccess() throws Exception {
        KinesisProperties properties = new KinesisProperties();
        properties.getConsumers().setMaxRetries(1);
        properties.getConsumers().setBackoffInitialMs(0);
        StreamEventHandler handler = Mockito.mock(StreamEventHandler.class);
        EventStreamPublisher publisher = Mockito.mock(EventStreamPublisher.class);
        RecordProcessorCheckpointer checkpointer = Mockito.mock(RecordProcessorCheckpointer.class);
        OrderStreamEvent event = OrderStreamEvent.create(
                "PAYMENT_COMPLETED", "test", "corr", "order-1",
                java.math.BigDecimal.TEN, "COMPLETED", List.of(), "auth", null);
        when(handler.validateAndProcess(anyString(), anyString(), anyString()))
                .thenThrow(new IllegalStateException("Base de datos temporalmente no disponible"))
                .thenReturn(event);
        GenericShardRecordProcessor processor = new GenericShardRecordProcessor(
                properties.getConsumers().getAnalyticsAppName(), "analytics", properties,
                handler, publisher, new ObjectMapper());
        KinesisClientRecord record = KinesisClientRecord.builder()
                .sequenceNumber("sequence-2")
                .data(ByteBuffer.wrap("{}".getBytes(StandardCharsets.UTF_8)))
                .partitionKey("partition")
                .build();

        processor.processRecords(ProcessRecordsInput.builder()
                .records(List.of(record))
                .checkpointer(checkpointer)
                .build());

        verify(handler, times(2)).validateAndProcess("{}", properties.getConsumers().getAnalyticsAppName(), "analytics");
        verify(checkpointer).checkpoint("sequence-2");
        verify(publisher, Mockito.never()).publishRawRecord(anyString(), anyString(), anyString());
    }

    @Test
    void doesNotCheckpointWhenDeadLetterPublicationFails() throws Exception {
        KinesisProperties properties = new KinesisProperties();
        StreamEventHandler handler = Mockito.mock(StreamEventHandler.class);
        EventStreamPublisher publisher = Mockito.mock(EventStreamPublisher.class);
        RecordProcessorCheckpointer checkpointer = Mockito.mock(RecordProcessorCheckpointer.class);
        when(handler.validateAndProcess(anyString(), anyString(), anyString()))
                .thenThrow(new NonRecoverableMessageException("Tipo inválido"));
        doThrow(new IllegalStateException("DLQ no disponible"))
                .when(publisher).publishRawRecord(anyString(), anyString(), anyString());
        GenericShardRecordProcessor processor = new GenericShardRecordProcessor(
                properties.getConsumers().getAnalyticsAppName(), "analytics", properties,
                handler, publisher, new ObjectMapper());
        KinesisClientRecord record = KinesisClientRecord.builder()
                .sequenceNumber("sequence-3")
                .data(ByteBuffer.wrap("{\"eventType\":\"BAD\"}".getBytes(StandardCharsets.UTF_8)))
                .partitionKey("partition")
                .build();

        processor.processRecords(ProcessRecordsInput.builder()
                .records(List.of(record))
                .checkpointer(checkpointer)
                .build());

        verify(checkpointer, Mockito.never()).checkpoint("sequence-3");
    }
}
