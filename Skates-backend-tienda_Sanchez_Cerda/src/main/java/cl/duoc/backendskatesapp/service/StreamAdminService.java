package cl.duoc.backendskatesapp.service;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.context.annotation.Profile;
import org.springframework.web.server.ResponseStatusException;

import cl.duoc.backendskatesapp.config.KinesisProperties;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.cloudwatch.model.ComparisonOperator;
import software.amazon.awssdk.services.cloudwatch.model.Dimension;
import software.amazon.awssdk.services.cloudwatch.model.GetMetricStatisticsRequest;
import software.amazon.awssdk.services.cloudwatch.model.PutMetricAlarmRequest;
import software.amazon.awssdk.services.cloudwatch.model.Statistic;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.CreateStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DeleteStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamSummaryRequest;
import software.amazon.awssdk.services.kinesis.model.IncreaseStreamRetentionPeriodRequest;
import software.amazon.awssdk.services.kinesis.model.InvalidArgumentException;
import software.amazon.awssdk.services.kinesis.model.ListStreamsRequest;
import software.amazon.awssdk.services.kinesis.model.LimitExceededException;
import software.amazon.awssdk.services.kinesis.model.ResourceInUseException;
import software.amazon.awssdk.services.kinesis.model.ScalingType;
import software.amazon.awssdk.services.kinesis.model.UpdateShardCountRequest;

@Service
@Profile({"all", "admin"})
public class StreamAdminService {
    private static final int AWS_MIN_RETENTION_HOURS = 24;
    private static final int AWS_MAX_RETENTION_HOURS = 8760;

    private final KinesisProperties properties;
    private final ObjectProvider<KinesisClient> kinesisProvider;
    private final ObjectProvider<DynamoDbAsyncClient> dynamoProvider;
    private final ObjectProvider<CloudWatchClient> cloudWatchProvider;

    public StreamAdminService(KinesisProperties properties, ObjectProvider<KinesisClient> kinesisProvider,
            ObjectProvider<DynamoDbAsyncClient> dynamoProvider,
            ObjectProvider<CloudWatchClient> cloudWatchProvider) {
        this.properties = properties;
        this.kinesisProvider = kinesisProvider;
        this.dynamoProvider = dynamoProvider;
        this.cloudWatchProvider = cloudWatchProvider;
    }

    public List<StreamSummary> listStreams() {
        KinesisClient client = kinesis();
        List<StreamSummary> streams = new ArrayList<>();
        String exclusiveStartName = null;
        do {
            var request = ListStreamsRequest.builder().limit(100);
            if (exclusiveStartName != null) request.exclusiveStartStreamName(exclusiveStartName);
            var response = client.listStreams(request.build());
            for (String name : response.streamNames()) {
                streams.add(describeStream(name));
            }
            exclusiveStartName = response.hasMoreStreams()
                    ? response.streamNames().get(response.streamNames().size() - 1) : null;
        } while (exclusiveStartName != null);
        return streams;
    }

    public StreamSummary describeStream(String name) {
        var summary = kinesis().describeStreamSummary(DescribeStreamSummaryRequest.builder()
                .streamName(name).build()).streamDescriptionSummary();
        return new StreamSummary(summary.streamName(), summary.streamStatusAsString(),
                summary.openShardCount(), summary.retentionPeriodHours(), capacityMode(summary.streamName()));
    }

    public StreamSummary createStream(String name, int shardCount, int retentionHours) {
        validateShardCount(shardCount);
        validateRetention(retentionHours);
        KinesisClient client = kinesis();
        client.createStream(CreateStreamRequest.builder().streamName(name).shardCount(shardCount).build());
        waitUntilActive(client, name);
        if (retentionHours != AWS_MIN_RETENTION_HOURS) {
            client.increaseStreamRetentionPeriod(IncreaseStreamRetentionPeriodRequest.builder()
                    .streamName(name).retentionPeriodHours(retentionHours).build());
        }
        return describeStream(name);
    }

    public void deleteStream(String name) {
        kinesis().deleteStream(DeleteStreamRequest.builder().streamName(name).enforceConsumerDeletion(true).build());
    }

    public StreamSummary updateShardCount(String name, int shardCount) {
        validateShardCount(shardCount);
        KinesisClient client = kinesis();
        try {
            client.updateShardCount(UpdateShardCountRequest.builder()
                    .streamName(name).targetShardCount(shardCount).scalingType(ScalingType.UNIFORM_SCALING).build());
        } catch (LimitExceededException | InvalidArgumentException exception) {
            throw new IllegalArgumentException("AWS rechazó el escalado uniforme de shards. "
                    + "Revise el estado ACTIVE y los límites de escalado Kinesis: " + exception.getMessage(), exception);
        }
        waitUntilShardCount(client, name, shardCount);
        return describeStream(name);
    }

    public StreamSummary updateRetention(String name, int retentionHours) {
        validateRetention(retentionHours);
        StreamSummary current = describeStream(name);
        try {
            if (retentionHours > current.retentionHours()) {
                kinesis().increaseStreamRetentionPeriod(IncreaseStreamRetentionPeriodRequest.builder()
                        .streamName(name).retentionPeriodHours(retentionHours).build());
            } else if (retentionHours < current.retentionHours()) {
                kinesis().decreaseStreamRetentionPeriod(
                        software.amazon.awssdk.services.kinesis.model.DecreaseStreamRetentionPeriodRequest.builder()
                                .streamName(name).retentionPeriodHours(retentionHours).build());
            }
        } catch (LimitExceededException | InvalidArgumentException exception) {
            throw new IllegalArgumentException("AWS rechazó el cambio de retención. "
                    + "Respete el rango de 24 a 8760 horas y las restricciones de frecuencia de AWS: "
                    + exception.getMessage(), exception);
        }
        return describeStream(name);
    }

    public List<StreamSummary> bootstrap() {
        KinesisClient client = kinesis();
        List<StreamSummary> ensured = new ArrayList<>();
        List<KinesisProperties.Stream> configured = List.of(
                properties.getOrders(), properties.getInventory(), properties.getDeadLetter());
        for (KinesisProperties.Stream stream : configured) {
            validateRetention(stream.getRetentionHours());
            try {
                client.describeStreamSummary(DescribeStreamSummaryRequest.builder()
                        .streamName(stream.getName()).build());
            } catch (software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException notFound) {
                try {
                    client.createStream(CreateStreamRequest.builder()
                            .streamName(stream.getName()).shardCount(stream.getShardCount()).build());
                } catch (ResourceInUseException alreadyCreating) {
                    // Another bootstrap process created the stream concurrently.
                }
            }
            waitUntilActive(client, stream.getName());
            updateRetention(stream.getName(), stream.getRetentionHours());
            ensured.add(describeStream(stream.getName()));
        }
        createIteratorAgeAlarm();
        return ensured;
    }

    public List<ConsumerLease> consumerLeases() {
        List<ConsumerLease> leases = new ArrayList<>();
        for (String applicationName : List.of(properties.getConsumers().getAnalyticsAppName(),
                properties.getConsumers().getNotificationsAppName())) {
            try {
                Map<String, AttributeValue> startKey = Map.of();
                do {
                    var request = ScanRequest.builder().tableName(applicationName);
                    if (!startKey.isEmpty()) request.exclusiveStartKey(startKey);
                    var response = dynamo().scan(request.build()).join();
                    for (Map<String, AttributeValue> item : response.items()) {
                        leases.add(new ConsumerLease(applicationName, value(item, "leaseKey"),
                                value(item, "leaseOwner"), value(item, "checkpoint")));
                    }
                    startKey = response.lastEvaluatedKey();
                } while (startKey != null && !startKey.isEmpty());
            } catch (RuntimeException exception) {
                if (!hasResourceNotFound(exception)) throw exception;
            }
        }
        return leases;
    }

    public Map<String, Object> iteratorAge(String streamName) {
        var result = cloudWatch().getMetricStatistics(GetMetricStatisticsRequest.builder()
                .namespace("AWS/Kinesis")
                .metricName("GetRecords.IteratorAgeMilliseconds")
                .dimensions(Dimension.builder().name("StreamName").value(streamName).build())
                .startTime(Instant.now().minus(Duration.ofMinutes(10)))
                .endTime(Instant.now())
                .period(properties.getAlarm().getPeriod())
                .statistics(Statistic.MAXIMUM)
                .build());
        Optional<Double> latest = result.datapoints().stream()
                .max((left, right) -> left.timestamp().compareTo(right.timestamp()))
                .map(point -> point.maximum());
        return Map.of("streamName", streamName, "iteratorAgeMilliseconds", latest.orElse(0.0));
    }

    private void createIteratorAgeAlarm() {
        cloudWatch().putMetricAlarm(PutMetricAlarmRequest.builder()
                .alarmName(properties.getOrders().getName() + "-iterator-age")
                .alarmDescription("Antigüedad de lectura de Kinesis sobre el umbral configurado")
                .namespace("AWS/Kinesis")
                .metricName("GetRecords.IteratorAgeMilliseconds")
                .dimensions(Dimension.builder().name("StreamName").value(properties.getOrders().getName()).build())
                .statistic(Statistic.MAXIMUM)
                .period(properties.getAlarm().getPeriod())
                .evaluationPeriods(properties.getAlarm().getEvaluationPeriods())
                .threshold((double) properties.getAlarm().getIteratorAgeThresholdMs())
                .comparisonOperator(ComparisonOperator.GREATER_THAN_THRESHOLD)
                .actionsEnabled(false)
                .build());
    }

    private void waitUntilActive(KinesisClient client, String name) {
        for (int attempt = 0; attempt < 120; attempt++) {
            var status = client.describeStreamSummary(DescribeStreamSummaryRequest.builder()
                    .streamName(name).build()).streamDescriptionSummary().streamStatusAsString();
            if ("ACTIVE".equals(status)) return;
            if ("DELETING".equals(status)) {
                throw new IllegalStateException("El stream " + name + " está siendo eliminado");
            }
            waitOneSecond(name);
        }
        throw new IllegalStateException("Tiempo de espera agotado esperando ACTIVE para el stream " + name);
    }

    private void waitUntilShardCount(KinesisClient client, String name, int targetShardCount) {
        for (int attempt = 0; attempt < 120; attempt++) {
            var summary = client.describeStreamSummary(DescribeStreamSummaryRequest.builder()
                    .streamName(name).build()).streamDescriptionSummary();
            if ("ACTIVE".equals(summary.streamStatusAsString())
                    && summary.openShardCount() == targetShardCount) return;
            if ("DELETING".equals(summary.streamStatusAsString())) {
                throw new IllegalStateException("El stream " + name + " está siendo eliminado");
            }
            waitOneSecond(name);
        }
        throw new IllegalStateException("Tiempo de espera agotado esperando " + targetShardCount
                + " shards activos en el stream " + name);
    }

    private void waitOneSecond(String name) {
        try {
            Thread.sleep(1000);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Se interrumpió la espera del stream " + name, exception);
        }
    }

    private void validateShardCount(int shardCount) {
        if (shardCount < 1 || shardCount > 10000) {
            throw new IllegalArgumentException("La cantidad de shards debe estar entre 1 y 10000");
        }
    }

    private void validateRetention(int hours) {
        if (hours < AWS_MIN_RETENTION_HOURS || hours > AWS_MAX_RETENTION_HOURS) {
            throw new IllegalArgumentException("La retención Kinesis debe estar entre "
                    + AWS_MIN_RETENTION_HOURS + " y " + AWS_MAX_RETENTION_HOURS + " horas");
        }
    }

    private String capacityMode(String name) {
        return List.of(properties.getOrders(), properties.getInventory(), properties.getDeadLetter()).stream()
                .filter(stream -> stream.getName().equals(name))
                .map(KinesisProperties.Stream::getCapacityMode)
                .findFirst()
                .orElse("PROVISIONED");
    }

    private String value(Map<String, AttributeValue> item, String key) {
        AttributeValue attribute = item.get(key);
        return attribute == null || attribute.s() == null ? "" : attribute.s();
    }

    private boolean hasResourceNotFound(Throwable exception) {
        Throwable current = exception;
        while (current != null) {
            if (current instanceof ResourceNotFoundException) return true;
            current = current.getCause();
        }
        return false;
    }

    private KinesisClient kinesis() {
        ensureEnabled();
        return kinesisProvider.getObject();
    }

    private DynamoDbAsyncClient dynamo() {
        ensureEnabled();
        return dynamoProvider.getObject();
    }

    private CloudWatchClient cloudWatch() {
        ensureEnabled();
        return cloudWatchProvider.getObject();
    }

    private void ensureEnabled() {
        if (!properties.isEnabled()) {
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE,
                    "Kinesis está deshabilitado. Configure KINESIS_ENABLED=true y los recursos AWS.");
        }
    }

    public record StreamSummary(String name, String status, Integer shardCount, Integer retentionHours, String capacityMode) { }
    public record ConsumerLease(String applicationName, String shardId, String leaseOwner, String checkpoint) { }
}
