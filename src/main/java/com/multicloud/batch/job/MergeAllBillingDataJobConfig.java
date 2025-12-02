package com.multicloud.batch.job;

import com.multicloud.batch.constant.BillingTypeConstant;
import com.multicloud.batch.enums.CloudProvider;
import com.multicloud.batch.helper.ServiceLevelBillingSql;
import com.multicloud.batch.model.ServiceLevelBilling;
import com.multicloud.batch.service.JobStepService;
import com.multicloud.batch.service.ServiceTypeService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDate;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim Molla
 * Email: shamim.molla@vivasoftltd.com
 */

@Slf4j
@Configuration
@RequiredArgsConstructor
@ConditionalOnExpression("${batch_job.merge_billing.enabled}")
public class MergeAllBillingDataJobConfig {

    private static final int CHUNK = 500;

    private final DataSource dataSource;
    private final JdbcTemplate jdbcTemplate;

    private final JobRepository jobRepository;
    private final PlatformTransactionManager platformTransactionManager;

    private final ServiceTypeService serviceTypeService;
    private final JobStepService jobStepService;

    @Bean
    public Job mergeAllBillingDataJob() {
        return new JobBuilder("mergeAllBillingDataJob", jobRepository)
                .start(fetchAndStoreServiceTypeStep())
                .next(mergeHuaweiBillingDataStep())
                .next(mergeHuaweiExtraBillingDataStep())
                .next(mergeGcpBillingDataStep())
                .next(mergeGcpExtraBillingDataStep())
                .next(mergeAwsBillingDataStep())
                .next(mergeAwsExtraBillingDataStep())
                .build();
    }

    @Bean
    public Step fetchAndStoreServiceTypeStep() {
        return new StepBuilder("fetchAndStoreServiceTypeStep", jobRepository)
                .tasklet((contribution, chunkContext) -> {

                    serviceTypeService.fetchAndStoreServiceTypeToMap();

                    return RepeatStatus.FINISHED;
                }, platformTransactionManager)
                .build();
    }

    @Bean
    public Step mergeHuaweiBillingDataStep() {
        return new StepBuilder("mergeHuaweiBillingDataStep", jobRepository)
                .<ServiceLevelBilling, ServiceLevelBilling>chunk(CHUNK, platformTransactionManager)
                .reader(huaweiDataReader())
                .processor(processBillingData())
                .writer(this::upsertAll)
                .build();
    }

    @Bean
    public Step mergeHuaweiExtraBillingDataStep() {
        return new StepBuilder("mergeHuaweiExtraBillingDataStep", jobRepository)
                .<ServiceLevelBilling, ServiceLevelBilling>chunk(CHUNK, platformTransactionManager)
                .reader(huaweiExtraDataReader())
                .processor(processBillingData())
                .writer(this::upsertAll)
                .build();
    }

    @Bean
    @StepScope
    public JdbcCursorItemReader<ServiceLevelBilling> huaweiDataReader() {

        try {
            return getBillingDataCursorItemReader(
                    ServiceLevelBillingSql.HUAWEI_SQL, "mergeHuaweiBillingDataStep", false
            );
        } catch (Exception e) {
            throw new IllegalStateException("Spring Batch configuration problem: ", e);
        }

    }

    @Bean
    @StepScope
    public JdbcCursorItemReader<ServiceLevelBilling> huaweiExtraDataReader() {

        try {
            return getBillingDataCursorItemReader(
                    ServiceLevelBillingSql.HUAWEI_EXTRA_LI_SQL,
                    "mergeHuaweiExtraBillingDataStep",
                    true
            );
        } catch (Exception e) {
            throw new IllegalStateException("Spring Batch configuration problem: ", e);
        }

    }

    @Bean
    public Step mergeGcpBillingDataStep() {
        return new StepBuilder("mergeGcpBillingDataStep", jobRepository)
                .<ServiceLevelBilling, ServiceLevelBilling>chunk(CHUNK, platformTransactionManager)
                .reader(gcpDataReader())
                .processor(processBillingData())
                .writer(this::upsertAll)
                .build();
    }

    @Bean
    public Step mergeGcpExtraBillingDataStep() {
        return new StepBuilder("mergeGcpExtraBillingDataStep", jobRepository)
                .<ServiceLevelBilling, ServiceLevelBilling>chunk(CHUNK, platformTransactionManager)
                .reader(gcpExtraDataReader())
                .processor(processBillingData())
                .writer(this::upsertAll)
                .build();
    }

    @Bean
    @StepScope
    public JdbcCursorItemReader<ServiceLevelBilling> gcpDataReader() {

        try {
            return getBillingDataCursorItemReader(
                    ServiceLevelBillingSql.GCP_SQL, "mergeGcpBillingDataStep", false
            );
        } catch (Exception e) {
            throw new IllegalStateException("Spring Batch configuration problem: ", e);
        }

    }

    @Bean
    @StepScope
    public JdbcCursorItemReader<ServiceLevelBilling> gcpExtraDataReader() {

        try {
            return getBillingDataCursorItemReader(
                    ServiceLevelBillingSql.GCP_EXTRA_LI_SQL,
                    "mergeGcpExtraBillingDataStep",
                    true
            );
        } catch (Exception e) {
            throw new IllegalStateException("Spring Batch configuration problem: ", e);
        }

    }

    @Bean
    public Step mergeAwsBillingDataStep() {
        return new StepBuilder("mergeAwsBillingDataStep", jobRepository)
                .<ServiceLevelBilling, ServiceLevelBilling>chunk(CHUNK, platformTransactionManager)
                .reader(awsDataReader())
                .processor(processBillingData())
                .writer(this::upsertAll)
                .build();
    }

    @Bean
    public Step mergeAwsExtraBillingDataStep() {
        return new StepBuilder("mergeAwsExtraBillingDataStep", jobRepository)
                .<ServiceLevelBilling, ServiceLevelBilling>chunk(CHUNK, platformTransactionManager)
                .reader(awsExtraDataReader())
                .processor(processBillingData())
                .writer(this::upsertAll)
                .build();
    }

    @Bean
    @StepScope
    public JdbcCursorItemReader<ServiceLevelBilling> awsDataReader() {

        try {
            return getBillingDataCursorItemReader(
                    ServiceLevelBillingSql.AWS_SQL, "mergeAwsBillingDataStep", false
            );
        } catch (Exception e) {
            throw new IllegalStateException("Spring Batch configuration problem: ", e);
        }

    }

    @Bean
    @StepScope
    public JdbcCursorItemReader<ServiceLevelBilling> awsExtraDataReader() {

        try {
            return getBillingDataCursorItemReader(
                    ServiceLevelBillingSql.AWS_EXTRA_LI_SQL,
                    "mergeAwsExtraBillingDataStep",
                    true
            );
        } catch (Exception e) {
            throw new IllegalStateException("Spring Batch configuration problem: ", e);
        }

    }

    private JdbcCursorItemReader<ServiceLevelBilling> getBillingDataCursorItemReader(String sql,
                                                                                     String stepName,
                                                                                     boolean isExtraDataReader) throws Exception {

        LocalDate endDate = LocalDate.now();
        LocalDate startDate = endDate.minusMonths(6).withDayOfMonth(1);

        // For now, we are going to update the full data set
//        boolean stepEverCompleted = jobStepService.hasStepEverCompleted(stepName);
//        if (stepEverCompleted) {
//            startDate = endDate.minusMonths(1).withDayOfMonth(1);
//        }

        JdbcCursorItemReader<ServiceLevelBilling> reader = new JdbcCursorItemReader<>();

        reader.setDataSource(dataSource);
        reader.setSql(sql);
        reader.setVerifyCursorPosition(false);
        reader.setSaveState(false);

        reader.setFetchSize(500);

        reader.setPreparedStatementSetter(ps -> {
            ps.setObject(1, startDate);

            if (!isExtraDataReader) {
                ps.setObject(2, endDate);
            }

        });

        reader.setRowMapper((rs, rowNum) -> ServiceLevelBilling.builder()
                .usageDate(rs.getDate("usage_date").toLocalDate())
                .cloudProvider(CloudProvider.valueOf(rs.getString("cloud_provider")))
                .billingAccountId(rs.getString("billing_account_id"))
                .usageAccountId(rs.getString("usage_account_id"))
                .usageAccountName(rs.getString("usage_account_name"))
                .serviceCode(rs.getString("service_code"))
                .serviceName(rs.getString("service_name"))
                .billingType(rs.getString("billing_type"))
                .isLiOutsideOfMonth(rs.getBoolean("is_li_outside_of_month"))
                .cost(rs.getBigDecimal("cost"))
                .extCost(rs.getBigDecimal("ext_cost"))
                .build());

        reader.afterPropertiesSet();

        return reader;
    }

    private ItemProcessor<ServiceLevelBilling, ServiceLevelBilling> processBillingData() {
        return item -> {

            String parentCategory = serviceTypeService.getParentCategory(
                    item.getServiceCode(), item.getCloudProvider()
            );

            if (parentCategory != null && parentCategory.isEmpty()) {

                if (BillingTypeConstant.REGULAR.contains(item.getBillingType())) {
                    parentCategory = "Regular";
                } else if (BillingTypeConstant.FEE.contains(item.getBillingType())) {
                    parentCategory = "Fee";
                } else if (BillingTypeConstant.DISCOUNT.contains(item.getBillingType())) {
                    parentCategory = "Discount";
                } else if (BillingTypeConstant.TAX.contains(item.getBillingType())) {
                    parentCategory = "Tax";
                } else {
                    parentCategory = "Other";
                }

            }

            item.setParentCategory(parentCategory);

            return item;
        };
    }

    private void upsertAll(Chunk<? extends ServiceLevelBilling> records) {

        if (records == null || records.isEmpty()) {
            return;
        }

        log.info("Upserting {} records...", records.size());

        jdbcTemplate.batchUpdate(
                ServiceLevelBillingSql.UPSERT_SQL,
                new BatchPreparedStatementSetter() {

                    @Override
                    public void setValues(PreparedStatement ps, int i) throws SQLException {

                        ServiceLevelBilling item = records.getItems().get(i);

                        ps.setDate(1, Date.valueOf(item.getUsageDate()));
                        ps.setString(2, item.getCloudProvider().name());
                        ps.setString(3, item.getBillingAccountId());
                        ps.setString(4, item.getUsageAccountId());
                        ps.setString(5, item.getUsageAccountName());
                        ps.setString(6, item.getServiceCode());
                        ps.setString(7, item.getServiceName());
                        ps.setString(8, item.getBillingType());
                        ps.setBoolean(9, item.isLiOutsideOfMonth());
                        ps.setString(10, item.getParentCategory());
                        ps.setBigDecimal(11, item.getCost());
                        ps.setBigDecimal(12, item.getExtCost());

                    }

                    @Override
                    public int getBatchSize() {
                        return records.size();
                    }

                }
        );

    }

}
