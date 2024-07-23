package org.example.bigqueryemulatorwrite;

import org.example.bigqueryemulatorwrite.repository.BigQueryRepository;
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

    protected static final String DATASET_NAME = "local-dataset";
    @Container
    protected static BigQueryEmulatorContainer bigQueryContainer = new BigQueryEmulatorContainer("ghcr.io/goccy/bigquery-emulator:0.6.3").withCommand(
            "--log-level=debug",
            "--project=test-project",
            "--dataset=local-dataset");
    @Autowired
    BigQueryRepository repository;

    @DynamicPropertySource
    static void setProperties(DynamicPropertyRegistry registry) {
        registry.add("bigquery.emulator.project-id", bigQueryContainer::getProjectId);
        registry.add("bigquery.emulator.host", bigQueryContainer::getEmulatorHttpEndpoint);
        registry.add("bigquery.emulator.grpcHost", () -> String.format("%s:%d", bigQueryContainer.getHost(), bigQueryContainer.getMappedPort(9060)));
        registry.add("bigquery.emulator.dataset", () -> DATASET_NAME);
        registry.add("bigquery.emulator.table", () -> "local-table");
        registry.add("spring.cloud.gcp.bigquery.datasetName", () -> DATASET_NAME);
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
