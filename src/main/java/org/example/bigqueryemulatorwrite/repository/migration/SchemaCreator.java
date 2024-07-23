package org.example.bigqueryemulatorwrite.repository.migration;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.PrimaryKey;
import com.google.cloud.bigquery.RangePartitioning;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableConstraints;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.TimePartitioning;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.example.bigqueryemulatorwrite.BigQueryEmulatorProperties;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Collections;

@Slf4j
@Component
public class SchemaCreator {
    public static final int PARTITIONS_COUNT = 40;

    @Autowired
    private BigQueryEmulatorProperties bigQueryProperties;

    @Autowired
    private BigQuery bigQuery;

    @PostConstruct
    public void create() {
        if (tableExists()) {
            return;
        }
        createTable();
    }

    private boolean tableExists() {
        log.info("Using {} dataset, {} table ", bigQueryProperties.getDataset(), bigQueryProperties.getTable());
        TableId tableId = TableId.of(bigQueryProperties.getDataset(), bigQueryProperties.getTable());
        Table table = bigQuery.getTable(tableId);
        return table != null && table.exists();
    }

    public void createTable() {
        Schema schema = Schema.of(Field.of("timestamp", StandardSQLTypeName.TIMESTAMP),
                Field.of("id", StandardSQLTypeName.STRING),
                Field.of("json", StandardSQLTypeName.JSON),
                Field.of("partition", StandardSQLTypeName.INT64));

        TableId tableId = TableId.of(bigQueryProperties.getDataset(), bigQueryProperties.getTable());

        RangePartitioning partitioning = RangePartitioning.newBuilder()
                .setField("partition")
                .setRange(RangePartitioning.Range.newBuilder().setStart(0L).setEnd((long) PARTITIONS_COUNT).setInterval(1L).build())
                .build();

        TimePartitioning timePartitioning = TimePartitioning.newBuilder(TimePartitioning.Type.DAY).setField("timestamp").build();
        PrimaryKey primaryKey = PrimaryKey.newBuilder().setColumns(Collections.singletonList("id")).build();
        TableConstraints tableConstraints = TableConstraints.newBuilder().setPrimaryKey(primaryKey).build();
        StandardTableDefinition tableDefinition = StandardTableDefinition.newBuilder()
                .setSchema(schema)
                .setRangePartitioning(partitioning)
                .setTimePartitioning(timePartitioning)
                .setTableConstraints(tableConstraints)
                .build();

        TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();
        bigQuery.create(tableInfo);
    }
}
