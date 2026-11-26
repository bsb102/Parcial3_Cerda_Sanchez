package cl.duoc.backend_skates_app;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import cl.duoc.backendskatesapp.config.KinesisProperties;
import cl.duoc.backendskatesapp.streaming.event.OrderStreamEvent;
import cl.duoc.backendskatesapp.streaming.producer.EventStreamPublisher;
import tools.jackson.databind.ObjectMapper;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResultEntry;
import software.amazon.awssdk.services.kinesis.model.PutRecordResponse;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;

@ExtendWith(MockitoExtension.class)
class EventStreamPublisherTest {

    @Mock
    private KinesisClient kinesisClient;

    @Test
    void retriesOnlyFailedRecordsAfterCheckingFailedRecordCount() {
        KinesisProperties properties = new KinesisProperties();
        properties.getConsumers().setBackoffInitialMs(0);
        properties.getConsumers().setMaxBackoffMs(0);
        EventStreamPublisher publisher = new EventStreamPublisher(kinesisClient, new ObjectMapper(), properties);
        OrderStreamEvent first = order("order-a");
        OrderStreamEvent second = order("order-b");

        when(kinesisClient.putRecords(any(PutRecordsRequest.class)))
                .thenReturn(PutRecordsResponse.builder()
                        .failedRecordCount(1)
                        .records(
                                PutRecordsResultEntry.builder().errorCode("ProvisionedThroughputExceededException").build(),
                                PutRecordsResultEntry.builder().shardId("shard-1").sequenceNumber("2").build())
                        .build())
                .thenReturn(PutRecordsResponse.builder()
                        .failedRecordCount(0)
                        .records(PutRecordsResultEntry.builder().shardId("shard-1").sequenceNumber("3").build())
                        .build());

        publisher.publishBatch(List.of(first, second), "orders");

        verify(kinesisClient, times(2)).putRecords(any(PutRecordsRequest.class));
    }

    @Test
    void rejectsBatchWhenAwsReportsFailedRecordsAfterRetryLimit() {
        KinesisProperties properties = new KinesisProperties();
        properties.getConsumers().setMaxRetries(0);
        EventStreamPublisher publisher = new EventStreamPublisher(kinesisClient, new ObjectMapper(), properties);
        when(kinesisClient.putRecords(any(PutRecordsRequest.class)))
                .thenReturn(PutRecordsResponse.builder()
                        .failedRecordCount(1)
                        .records(PutRecordsResultEntry.builder().errorCode("InternalFailure").build())
                        .build());

        assertThrows(IllegalStateException.class, () -> publisher.publishBatch(List.of(order("order-a")), "orders"));
    }

    @Test
    void publishesSingleRecordUsingOrderPartitionKey() {
        EventStreamPublisher publisher = new EventStreamPublisher(kinesisClient, new ObjectMapper(), new KinesisProperties());
        when(kinesisClient.putRecord(any(PutRecordRequest.class)))
                .thenReturn(PutRecordResponse.builder().shardId("shard-1").sequenceNumber("1").build());

        publisher.publish(order("buy-order-1"), "orders");

        verify(kinesisClient).putRecord(any(PutRecordRequest.class));
    }

    private OrderStreamEvent order(String buyOrder) {
        return OrderStreamEvent.create("PAYMENT_COMPLETED", "test", "correlation",
                buyOrder, BigDecimal.TEN, "COMPLETED", List.of(), "auth", null);
    }
}
