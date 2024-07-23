package org.example.bigqueryemulatorwrite.config;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.cloud.NoCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteSettings;
import com.google.cloud.bigquery.storage.v1.stub.EnhancedBigQueryReadStubSettings;
import io.grpc.ManagedChannelBuilder;
import lombok.RequiredArgsConstructor;
import org.example.bigqueryemulatorwrite.BigQueryEmulatorProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

import java.io.IOException;

@Configuration
@RequiredArgsConstructor
@Profile({"test"})
public class BigQueryEmulatorConfig {

    private final BigQueryEmulatorProperties properties;

    @Bean
    public BigQuery bigQuery() {
        BigQueryOptions.Builder bigQueryBuilder = BigQueryOptions.newBuilder().setProjectId(properties.getProjectId());
        bigQueryBuilder.setHost(properties.getHost());
        bigQueryBuilder.setProjectId(properties.getProjectId());
        bigQueryBuilder.setCredentials(NoCredentials.getInstance());

        return bigQueryBuilder.build().getService();
    }

    @Bean
    public BigQueryWriteClient createBigQueryWriteClient() throws IOException {
        BigQueryWriteSettings settings = BigQueryWriteSettings.newBuilder()
                .setEndpoint(properties.getGrpcHost())
                .setCredentialsProvider(NoCredentialsProvider.create())
                .setTransportChannelProvider(
                        EnhancedBigQueryReadStubSettings.defaultGrpcTransportProviderBuilder()
                                .setChannelConfigurator(ManagedChannelBuilder::usePlaintext)
                                .build()
                )
                .build();

        return BigQueryWriteClient.create(settings);
    }

    /**
     * Overrides provider specified in GcpBigQueryAutoConfiguration to avoid potential conflicts
     *
     * @return no credentials provider
     */
    @Bean
    CredentialsProvider googleCredentials() {
        return NoCredentials::getInstance;
    }
}
