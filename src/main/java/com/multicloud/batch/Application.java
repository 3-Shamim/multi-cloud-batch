package com.multicloud.batch;

import jakarta.annotation.PostConstruct;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.scheduling.annotation.EnableScheduling;

import javax.sql.DataSource;

@SpringBootApplication
@EnableScheduling
public class Application {

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

//    @PostConstruct
//    void init() {
//
//        Resource resource = new ClassPathResource("org/springframework/batch/core/schema-mysql.sql");
//        System.out.println(resource.exists());
//
//    }

//    @Bean
//    public CommandLineRunner runBatchSchemaInit(DataSource dataSource, JdbcTemplate jdbcTemplate) {
//        return args -> {
//            Integer count = jdbcTemplate.queryForObject(
//                    "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'BATCH_JOB_INSTANCE'",
//                    Integer.class
//            );
//            if (count == null || count == 0) {
//                Resource resource = new ClassPathResource("org/springframework/batch/core/schema-mysql.sql");
//                ResourceDatabasePopulator populator = new ResourceDatabasePopulator(resource);
//                populator.execute(dataSource);
//                System.out.println("✅ Spring Batch schema created (conditional).");
//            }
//        };
//    }

}
