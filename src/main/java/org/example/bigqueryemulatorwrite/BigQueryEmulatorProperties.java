package org.example.bigqueryemulatorwrite;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Getter
@Setter
@ToString
@ConfigurationProperties("bigquery.emulator")
public class BigQueryEmulatorProperties {
    private String dataset;
    private String table;
    private String projectId;
    private String host;
    private String grpcHost;
}

