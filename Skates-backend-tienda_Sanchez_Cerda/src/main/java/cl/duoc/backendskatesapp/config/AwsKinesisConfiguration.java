package cl.duoc.backendskatesapp.config;

import java.net.URI;
import java.time.Duration;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisClient;

@Configuration
@ConditionalOnProperty(name = "app.kinesis.enabled", havingValue = "true")
public class AwsKinesisConfiguration {

    private ClientOverrideConfiguration clientOverrides() {
        return ClientOverrideConfiguration.builder()
                .apiCallAttemptTimeout(Duration.ofSeconds(10))
                .apiCallTimeout(Duration.ofSeconds(30))
                .build();
    }

    private URI endpoint(KinesisProperties properties) {
        return properties.getEndpointOverride().isBlank() ? null : URI.create(properties.getEndpointOverride());
    }

    @Bean
    KinesisClient kinesisClient(KinesisProperties properties) {
        var builder = KinesisClient.builder().region(Region.of(properties.getRegion()))
                .credentialsProvider(DefaultCredentialsProvider.create())
                .overrideConfiguration(clientOverrides());
        if (endpoint(properties) != null) builder.endpointOverride(endpoint(properties));
        return builder.build();
    }

    @Bean
    KinesisAsyncClient kinesisAsyncClient(KinesisProperties properties) {
        var builder = KinesisAsyncClient.builder().region(Region.of(properties.getRegion()))
                .credentialsProvider(DefaultCredentialsProvider.create())
                .overrideConfiguration(clientOverrides());
        if (endpoint(properties) != null) builder.endpointOverride(endpoint(properties));
        return builder.build();
    }

    @Bean
    DynamoDbAsyncClient dynamoDbAsyncClient(KinesisProperties properties) {
        var builder = DynamoDbAsyncClient.builder().region(Region.of(properties.getRegion()))
                .credentialsProvider(DefaultCredentialsProvider.create())
                .overrideConfiguration(clientOverrides());
        if (endpoint(properties) != null) builder.endpointOverride(endpoint(properties));
        return builder.build();
    }

    @Bean
    CloudWatchClient cloudWatchClient(KinesisProperties properties) {
        var builder = CloudWatchClient.builder().region(Region.of(properties.getRegion()))
                .credentialsProvider(DefaultCredentialsProvider.create())
                .overrideConfiguration(clientOverrides());
        if (endpoint(properties) != null) builder.endpointOverride(endpoint(properties));
        return builder.build();
    }

    @Bean
    CloudWatchAsyncClient cloudWatchAsyncClient(KinesisProperties properties) {
        var builder = CloudWatchAsyncClient.builder().region(Region.of(properties.getRegion()))
                .credentialsProvider(DefaultCredentialsProvider.create())
                .overrideConfiguration(clientOverrides());
        if (endpoint(properties) != null) builder.endpointOverride(endpoint(properties));
        return builder.build();
    }
}
