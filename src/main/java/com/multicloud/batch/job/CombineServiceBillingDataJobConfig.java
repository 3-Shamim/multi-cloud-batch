package com.multicloud.batch.job;

import com.multicloud.batch.helper.ServiceLevelBillingSql;
import com.multicloud.batch.model.ServiceLevelBilling;
import lombok.RequiredArgsConstructor;
import org.jetbrains.annotations.NotNull;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim Molla
 * Email: shamim.molla@vivasoftltd.com
 */

@Configuration
@RequiredArgsConstructor
public class CombineServiceBillingDataJobConfig {

    private static final int CHUNK = 200;

    private final DataSource dataSource;
    private final JdbcTemplate jdbcTemplate;

    private final JobRepository jobRepository;
    private final PlatformTransactionManager platformTransactionManager;

    @Bean
    public Job combineServiceBillingDataJob() {
        return new JobBuilder("combineServiceBillingDataJob", jobRepository)
                .start(combineServiceHuaweiBillingDataStep())
                .next(combineServiceGcpBillingDataStep())
                .next(combineServiceAwsBillingDataStep())
                .build();
    }

    @Bean
    public Step combineServiceHuaweiBillingDataStep() {
        return new StepBuilder("combineServiceHuaweiBillingDataStep", jobRepository)
                .<ServiceLevelBilling, ServiceLevelBilling>chunk(CHUNK, platformTransactionManager)
                .reader(huaweiDataReader())
                .writer(this::upsertAll)
                .build();
    }

    @Bean
    public ItemReader<ServiceLevelBilling> huaweiDataReader() {

        try {
            return getBillingDataCursorItemReader(ServiceLevelBillingSql.HUAWEI_SQL);
        } catch (Exception e) {
            throw new IllegalStateException("Spring Batch configuration problem: ", e);
        }

    }

    @Bean
    public Step combineServiceGcpBillingDataStep() {
        return new StepBuilder("combineServiceGcpBillingDataStep", jobRepository)
                .<ServiceLevelBilling, ServiceLevelBilling>chunk(CHUNK, platformTransactionManager)
                .reader(gcpDataReader())
                .writer(this::upsertAll)
                .build();
    }

    @Bean
    public ItemReader<ServiceLevelBilling> gcpDataReader() {

        try {
            return getBillingDataCursorItemReader(ServiceLevelBillingSql.GCP_SQL);
        } catch (Exception e) {
            throw new IllegalStateException("Spring Batch configuration problem: ", e);
        }

    }

    @Bean
    public Step combineServiceAwsBillingDataStep() {
        return new StepBuilder("combineServiceAwsBillingDataStep", jobRepository)
                .<ServiceLevelBilling, ServiceLevelBilling>chunk(CHUNK, platformTransactionManager)
                .reader(awsDataReader())
                .writer(this::upsertAll)
                .build();
    }

    @Bean
    public ItemReader<ServiceLevelBilling> awsDataReader() {

        try {
            return getBillingDataCursorItemReader(ServiceLevelBillingSql.AWS_SQL);
        } catch (Exception e) {
            throw new IllegalStateException("Spring Batch configuration problem: ", e);
        }

    }

    private JdbcCursorItemReader<ServiceLevelBilling> getBillingDataCursorItemReader(String sql) throws Exception {

        JdbcCursorItemReader<ServiceLevelBilling> reader = new JdbcCursorItemReader<>();

        reader.setDataSource(dataSource);
        reader.setSql(sql);

        reader.setFetchSize(1000);

        reader.setRowMapper((rs, rowNum) -> ServiceLevelBilling.builder()
                .organizationId(rs.getLong("organization_id"))
                .billingAccountId(rs.getString("billing_account_id"))
                .usageAccountId(rs.getString("usage_account_id"))
                .usageAccountName(rs.getString("usage_account_name"))
                .serviceCode(rs.getString("service_code"))
                .serviceName(rs.getString("service_name"))
                .cost(rs.getBigDecimal("cost"))
                .build());

        reader.afterPropertiesSet();

        return reader;
    }

    private void upsertAll(Chunk<? extends ServiceLevelBilling> records) {

        if (records == null || records.isEmpty()) {
            return;
        }

        jdbcTemplate.batchUpdate(
                ServiceLevelBillingSql.UPSERT_SQL,
                new BatchPreparedStatementSetter() {

                    @Override
                    public void setValues(@NotNull PreparedStatement ps, int i) throws SQLException {

                        ServiceLevelBilling item = records.getItems().get(i);

                        ps.setLong(1, item.getOrganizationId());
                        ps.setString(2, item.getCloudProvider().name());
                        ps.setDate(3, Date.valueOf(item.getUsageDate()));
                        ps.setString(4, item.getBillingAccountId());
                        ps.setString(5, item.getUsageAccountId());
                        ps.setString(6, item.getUsageAccountName());
                        ps.setString(7, item.getServiceCode());
                        ps.setString(8, item.getServiceName());
                        ps.setBigDecimal(9, item.getCost());
                    }

                    @Override
                    public int getBatchSize() {
                        return records.size();
                    }

                }
        );

    }

}