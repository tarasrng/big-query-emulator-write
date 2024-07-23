package org.example.bigqueryemulatorwrite;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class BigQueryEmulatorWriteApplication {

    public static void main(String[] args) {
        SpringApplication.run(BigQueryEmulatorWriteApplication.class, args);
    }
}
