package org.example.bigqueryemulatorwrite.repository;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.core.ApiFuture;
import com.google.api.gax.core.FixedExecutorProvider;
import com.google.api.gax.retrying.RetrySettings;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteSettings;
import com.google.cloud.bigquery.storage.v1.JsonStreamWriter;
import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import com.google.cloud.bigquery.storage.v1.TableName;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.protobuf.Descriptors;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.example.bigqueryemulatorwrite.BigQueryEmulatorProperties;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.stereotype.Repository;
import org.threeten.bp.Duration;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

@Slf4j
@Repository
@RequiredArgsConstructor
public class BigQueryRepository {

    private static final int DEFAULT_PARTITION = 0;
    private static final String CHANGE_TYPE_PSEUDO_COLUMN = "_change_type";
    private final BigQuery bigQuery;
    private final BigQueryWriteClient bigQueryWriteClient;
    private final BigQueryEmulatorProperties bigQueryProperties;
    private final ObjectMapper objectMapper;

    @SneakyThrows
    public void save(long timestamp, String deviceIdentifier, Map<String, Object> json) {
        int partition = calculatePartition();

        JSONArray rowContentArray = new JSONArray();
        JSONObject rowContent;
        rowContent = new JSONObject();

        rowContent.put("timestamp", DateTimeFormatter.ISO_INSTANT.format(Instant.ofEpochMilli(timestamp).atOffset(ZoneOffset.UTC)));
        rowContent.put("id", deviceIdentifier);
        rowContent.put("json", objectMapper.writeValueAsString(json));
        rowContent.put("partition", partition);
        //            rowContent.put(CHANGE_TYPE_PSEUDO_COLUMN, "UPSERT");

        rowContentArray.put(rowContent);

        RetrySettings retrySettings = RetrySettings.newBuilder()
                .setInitialRetryDelay(Duration.ofMillis(500))
                .setRetryDelayMultiplier(1.1)
                .setMaxAttempts(5)
                .setMaxRetryDelay(Duration.ofMinutes(1))
                .build();

        TableSchema tableSchema = TableSchema.newBuilder()
                .addFields(TableFieldSchema.newBuilder()
                        .setName("id")
                        .setType(TableFieldSchema.Type.STRING)
                        .setMode(TableFieldSchema.Mode.NULLABLE)
                        .build())
                .addFields(TableFieldSchema.newBuilder()
                        .setName("timestamp")
                        .setType(TableFieldSchema.Type.TIMESTAMP)
                        .setMode(TableFieldSchema.Mode.NULLABLE)
                        .build())
                .addFields(TableFieldSchema.newBuilder().setName("json").setType(TableFieldSchema.Type.JSON).setMode(TableFieldSchema.Mode.NULLABLE).build())
                .addFields(TableFieldSchema.newBuilder()
                        .setName("partition")
                        .setType(TableFieldSchema.Type.INT64)
                        .setMode(TableFieldSchema.Mode.NULLABLE)
                        .build())
                //                        .addFields(
                //                                TableFieldSchema.newBuilder()
                //                                        .setName(CHANGE_TYPE_PSEUDO_COLUMN)
                //                                        .setType(TableFieldSchema.Type.STRING)
                //                                        .setMode(Mode.NULLABLE)
                //                                        .build())
                .build();
        AppendRowsResponse response;
        TableName parentTable = TableName.of(bigQuery.getOptions().getProjectId(), bigQueryProperties.getDataset(), bigQueryProperties.getTable());
        try (JsonStreamWriter writer = JsonStreamWriter.newBuilder(parentTable.toString(), tableSchema, bigQueryWriteClient)
                .setExecutorProvider(FixedExecutorProvider.create(Executors.newScheduledThreadPool(100)))
                .setChannelProvider(BigQueryWriteSettings.defaultGrpcTransportProviderBuilder()
                        .setKeepAliveTime(org.threeten.bp.Duration.ofMinutes(1))
                        .setKeepAliveTimeout(org.threeten.bp.Duration.ofMinutes(1))
                        .setKeepAliveWithoutCalls(true)
                        .setChannelsPerCpu(2)
                        .build())
                .setEnableConnectionPool(true)
                .setRetrySettings(retrySettings)
                .build()) {

            ApiFuture<AppendRowsResponse> future = writer.append(rowContentArray);
            response = future.get();
        } catch (Descriptors.DescriptorValidationException | IOException | InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }

        if (response.getRowErrorsCount() > 0) {
            String errorMessages =
                    response.getRowErrorsList().stream().map(error -> error.getIndex() + ": " + error.getMessage()).collect(Collectors.joining("\n"));

            log.error("Error inserting row: {}", errorMessages);
        }
    }

    private int calculatePartition() {
        //stub for now
        return 1;
    }
}
