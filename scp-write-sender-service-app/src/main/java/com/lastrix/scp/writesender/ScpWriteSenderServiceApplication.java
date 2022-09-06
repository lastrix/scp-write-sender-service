package com.lastrix.scp.writesender;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = "com.lastrix.scp.writesender")
public class ScpWriteSenderServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(ScpWriteSenderServiceApplication.class, args);
    }

}
