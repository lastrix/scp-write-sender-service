package com.lastrix.scp.writesender;

import com.lastrix.scp.lib.db.SchemaInit;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Import;

@SpringBootApplication(scanBasePackages = "com.lastrix.scp.writesender")
public class ScpWriteSenderServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(ScpWriteSenderServiceApplication.class, args);
    }

}
