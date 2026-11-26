package cl.duoc.backend_skates_app;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

import cl.duoc.backendskatesapp.config.KinesisProperties;
import cl.duoc.backendskatesapp.service.StreamAdminService;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisClient;

class StreamAdminServiceTest {
    @Test
    void returns503WithoutCallingAwsWhenKinesisIsDisabled() {
        KinesisClient kinesis = mock(KinesisClient.class);
        DynamoDbAsyncClient dynamo = mock(DynamoDbAsyncClient.class);
        CloudWatchClient cloudWatch = mock(CloudWatchClient.class);
        StreamAdminService service = new StreamAdminService(new KinesisProperties(),
                provider(kinesis), provider(dynamo), provider(cloudWatch));

        ResponseStatusException exception = assertThrows(ResponseStatusException.class, service::listStreams);

        assertEquals(HttpStatus.SERVICE_UNAVAILABLE, exception.getStatusCode());
        verifyNoInteractions(kinesis, dynamo, cloudWatch);
    }

    private <T> ObjectProvider<T> provider(T bean) {
        @SuppressWarnings("unchecked")
        ObjectProvider<T> provider = mock(ObjectProvider.class);
        org.mockito.Mockito.when(provider.getObject()).thenReturn(bean);
        return provider;
    }
}
