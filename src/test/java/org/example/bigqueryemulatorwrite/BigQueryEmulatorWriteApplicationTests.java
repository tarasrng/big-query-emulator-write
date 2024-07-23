package org.example.bigqueryemulatorwrite;

import com.google.cloud.NoCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.DatasetInfo;
import org.example.bigqueryemulatorwrite.repository.BigQueryRepository;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.BigQueryEmulatorContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

@SpringBootTest
@Testcontainers
@ActiveProfiles("test")
class BigQueryEmulatorWriteApplicationTests {

    @Autowired
    BigQueryRepository repository;

    protected static final String DATASET_NAME = "local-bigquery";

    @Container
    protected static BigQueryEmulatorContainer bigQueryContainer = new BigQueryEmulatorContainer("ghcr.io/goccy/bigquery-emulator:0.6.3");

    @BeforeAll
    static void createDataset() {
        BigQuery bigQuery = BigQueryOptions.newBuilder()
                .setProjectId(bigQueryContainer.getProjectId())
                .setHost(bigQueryContainer.getEmulatorHttpEndpoint())
                .setCredentials(NoCredentials.getInstance())
                .build()
                .getService();

        DatasetInfo datasetInfo = DatasetInfo.newBuilder(DATASET_NAME).build();
        bigQuery.create(datasetInfo);
    }

    @DynamicPropertySource
    static void setProperties(DynamicPropertyRegistry registry) {
        registry.add("bigquery.emulator.project-id", bigQueryContainer::getProjectId);
        registry.add("bigquery.emulator.host", bigQueryContainer::getEmulatorHttpEndpoint);
        registry.add("bigquery.emulator.grpcHost", () -> String.format("%s:%d", bigQueryContainer.getHost(), bigQueryContainer.getMappedPort(9060)));
        registry.add("bigquery.emulator.dataset", () -> DATASET_NAME);
        registry.add("bigquery.emulator.table", () -> "local-table");
        registry.add("spring.cloud.gcp.bigquery.datasetName",() -> DATASET_NAME);
    }

    @Test
    void saveTest() {
        String id = "1";
        Map<String, Object> record = new HashMap<>();
        record.put("id", id);
        record.put("CountryCode", "br");

        repository.save(Instant.now().toEpochMilli(), id, record);
    }
}
