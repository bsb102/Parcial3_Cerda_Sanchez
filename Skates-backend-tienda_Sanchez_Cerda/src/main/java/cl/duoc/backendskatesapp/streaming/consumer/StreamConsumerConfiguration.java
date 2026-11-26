package cl.duoc.backendskatesapp.streaming.consumer;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import jakarta.annotation.PreDestroy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import cl.duoc.backendskatesapp.config.KinesisProperties;
import cl.duoc.backendskatesapp.streaming.producer.EventStreamPublisher;
import tools.jackson.databind.ObjectMapper;
import software.amazon.kinesis.common.InitialPositionInStream;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.coordinator.Scheduler;
import software.amazon.kinesis.processor.ShardRecordProcessorFactory;
import software.amazon.kinesis.common.ConfigsBuilder;
import software.amazon.kinesis.retrieval.polling.PollingConfig;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;

@Component
@Profile("stream-consumer")
@ConditionalOnProperty(name = "app.kinesis.enabled", havingValue = "true")
public class StreamConsumerConfiguration {
    private static final Logger logger = LoggerFactory.getLogger(StreamConsumerConfiguration.class);
    private final List<Scheduler> schedulers = new ArrayList<>();
    private final ExecutorService executor = Executors.newCachedThreadPool();

    public StreamConsumerConfiguration(KinesisProperties properties, KinesisAsyncClient kinesisClient,
            DynamoDbAsyncClient dynamoDbClient, CloudWatchAsyncClient cloudWatchClient,
            StreamEventHandler eventHandler, EventStreamPublisher publisher, ObjectMapper objectMapper) {
        String selector = properties.getConsumers().getAppSelector().toLowerCase();
        if (selector.equals("analytics") || selector.equals("all")) {
            schedulers.add(createScheduler(properties, kinesisClient, dynamoDbClient, cloudWatchClient,
                    eventHandler, publisher, objectMapper, "analytics",
                    properties.getConsumers().getAnalyticsAppName()));
        }
        if (selector.equals("notifications") || selector.equals("all")) {
            schedulers.add(createScheduler(properties, kinesisClient, dynamoDbClient, cloudWatchClient,
                    eventHandler, publisher, objectMapper, "notifications",
                    properties.getConsumers().getNotificationsAppName()));
        }
        if (schedulers.isEmpty()) {
            throw new IllegalArgumentException("KINESIS_CONSUMER_APP debe ser analytics, notifications o all");
        }
        for (Scheduler scheduler : schedulers) {
            executor.submit(() -> {
                logger.info("Iniciando consumidor KCL sobre el stream de órdenes");
                scheduler.run();
            });
        }
    }

    private Scheduler createScheduler(KinesisProperties properties, KinesisAsyncClient kinesisClient,
            DynamoDbAsyncClient dynamoDbClient, CloudWatchAsyncClient cloudWatchClient,
            StreamEventHandler eventHandler, EventStreamPublisher publisher, ObjectMapper objectMapper,
            String consumerType, String applicationName) {
        ShardRecordProcessorFactory factory = () -> new GenericShardRecordProcessor(
                applicationName, consumerType, properties, eventHandler, publisher, objectMapper);
        ConfigsBuilder configs = new ConfigsBuilder(
                properties.getOrders().getName(), applicationName, kinesisClient, dynamoDbClient,
                cloudWatchClient, UUID.randomUUID().toString(), factory);
        configs.leaseManagementConfig().failoverTimeMillis(properties.getConsumers().getFailoverTimeMillis());
        var retrieval = configs.retrievalConfig();
        retrieval.initialPositionInStreamExtended(
                InitialPositionInStreamExtended.newInitialPosition(
                        InitialPositionInStream.valueOf(properties.getConsumers().getInitialPosition())));
        retrieval.retrievalSpecificConfig(new PollingConfig(kinesisClient)
                .maxRecords(properties.getConsumers().getPollingMaxRecords()));
        return new Scheduler(configs.checkpointConfig(), configs.coordinatorConfig(),
                configs.leaseManagementConfig(), configs.lifecycleConfig(), configs.metricsConfig(),
                configs.processorConfig(), retrieval);
    }

    @PreDestroy
    public void stop() {
        schedulers.forEach(Scheduler::shutdown);
        executor.shutdown();
    }
}
