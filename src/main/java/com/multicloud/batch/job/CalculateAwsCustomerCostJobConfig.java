package com.multicloud.batch.job;

import com.multicloud.batch.dto.ProductDTO;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim Molla
 * Email: shamim.molla@vivasoftltd.com
 */

@Slf4j
@Configuration
@ConditionalOnExpression("${batch_job.aws_customer_cost.enabled}")
public class CalculateAwsCustomerCostJobConfig {

    private final DataSource dataSource;
    private final DataSource secondaryDataSource;

    private final JobRepository jobRepository;

    @Qualifier(value = "secondaryTransactionManager")
    private final PlatformTransactionManager platformTransactionManager;

    public CalculateAwsCustomerCostJobConfig(DataSource dataSource,
                                             DataSource secondaryDataSource,
                                             JobRepository jobRepository,
                                             PlatformTransactionManager platformTransactionManager) {
        this.dataSource = dataSource;
        this.secondaryDataSource = secondaryDataSource;
        this.jobRepository = jobRepository;
        this.platformTransactionManager = platformTransactionManager;
    }

    @Bean
    public Job calculateAwsCustomerCostJob() {

        return new JobBuilder("calculateAwsCustomerCostJob", jobRepository)
                .start(calculateAwsCustomerCostStep())
                .build();
    }

    @Bean
    public Step calculateAwsCustomerCostStep() {

        return new StepBuilder("calculateAwsCustomerCostStep", jobRepository)
                .<ProductDTO, ProductDTO>chunk(1, platformTransactionManager)
                .reader(awsCustomerCostProductReader())
                .writer(chunk -> {



                    System.out.println(chunk);
                })
                .build();
    }

    @Bean
    public ItemReader<ProductDTO> awsCustomerCostProductReader() {

        JdbcCursorItemReader<ProductDTO> reader = new JdbcCursorItemReader<>();
        reader.setDataSource(dataSource);
        reader.setSql("""
                    SELECT p.id, p.organization_id, o.internal, o.exceptional
                    FROM products p
                        JOIN organizations o ON p.organization_id = o.id
                    WHERE o.internal = false
                """);
        reader.setFetchSize(500);
        reader.setSaveState(false);
        reader.setVerifyCursorPosition(false);

        reader.setRowMapper((rs, rowNum) -> new ProductDTO(
                rs.getLong("id"),
                rs.getLong("organization_id"),
                rs.getBoolean("internal"),
                rs.getBoolean("exceptional")
        ));

        try {
            reader.afterPropertiesSet();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return reader;
    }

}
