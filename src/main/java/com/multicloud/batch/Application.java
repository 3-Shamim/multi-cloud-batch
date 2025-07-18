package com.multicloud.batch;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableAsync
@EnableScheduling
@SpringBootApplication
public class Application {

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

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
