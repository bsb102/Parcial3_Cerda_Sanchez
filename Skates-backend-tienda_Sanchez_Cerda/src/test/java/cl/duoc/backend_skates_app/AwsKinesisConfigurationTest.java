package cl.duoc.backend_skates_app;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.context.ConfigurationPropertiesAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import cl.duoc.backendskatesapp.config.AwsKinesisConfiguration;
import cl.duoc.backendskatesapp.config.KinesisProperties;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisClient;

class AwsKinesisConfigurationTest {
    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withConfiguration(
                    org.springframework.boot.autoconfigure.AutoConfigurations.of(
                            ConfigurationPropertiesAutoConfiguration.class))
            .withUserConfiguration(KinesisTestConfiguration.class)
            .withPropertyValues("app.kinesis.enabled=false");

    @Test
    void disabledFeatureBindsDefaultsWithoutCreatingAwsClients() {
        contextRunner.run(context -> {
            assertNotNull(context.getBean(KinesisProperties.class));
            assertFalse(context.getBean(KinesisProperties.class).isEnabled());
            assertFalse(context.containsBean("kinesisClient"));
            assertFalse(context.containsBean("kinesisAsyncClient"));
            assertFalse(context.containsBean("dynamoDbAsyncClient"));
            assertFalse(context.containsBean("cloudWatchClient"));
            assertFalse(context.containsBean("cloudWatchAsyncClient"));
        });
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(KinesisProperties.class)
    @Import(AwsKinesisConfiguration.class)
    static class KinesisTestConfiguration { }
}
