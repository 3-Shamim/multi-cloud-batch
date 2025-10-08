//package com.multicloud.batch.job;
//
//import com.multicloud.batch.enums.CloudProvider;
//import com.multicloud.batch.helper.ServiceLevelBillingSql;
//import com.multicloud.batch.model.ServiceLevelBilling;
//import com.multicloud.batch.service.JobStepService;
//import com.multicloud.batch.service.OrganizationService;
//import com.multicloud.batch.service.ProductAccountService;
//import com.multicloud.batch.service.ServiceTypeService;
//import lombok.RequiredArgsConstructor;
//import lombok.extern.slf4j.Slf4j;
//import org.springframework.batch.core.Job;
//import org.springframework.batch.core.JobParametersInvalidException;
//import org.springframework.batch.core.JobParametersValidator;
//import org.springframework.batch.core.Step;
//import org.springframework.batch.core.configuration.annotation.StepScope;
//import org.springframework.batch.core.job.builder.JobBuilder;
//import org.springframework.batch.core.repository.JobRepository;
//import org.springframework.batch.core.step.builder.StepBuilder;
//import org.springframework.batch.item.Chunk;
//import org.springframework.batch.item.database.JdbcCursorItemReader;
//import org.springframework.batch.repeat.RepeatStatus;
//import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
//import org.springframework.context.annotation.Bean;
//import org.springframework.context.annotation.Configuration;
//import org.springframework.jdbc.core.BatchPreparedStatementSetter;
//import org.springframework.jdbc.core.JdbcTemplate;
//import org.springframework.transaction.PlatformTransactionManager;
//import org.springframework.util.StringUtils;
//
//import javax.sql.DataSource;
//import java.sql.Date;
//import java.sql.PreparedStatement;
//import java.sql.SQLException;
//import java.time.LocalDate;
//import java.util.List;
//import java.util.Map;
//import java.util.stream.Collectors;
//
///**
// * Created by IntelliJ IDEA.
// * User: Md. Shamim Molla
// * Email: shamim.molla@vivasoftltd.com
// */
//
//@Slf4j
//@Configuration
//@RequiredArgsConstructor
//@ConditionalOnExpression("${batch_job.merge_billing.enabled}")
//public class MergeBillingDataJobConfigCopy {
//
//    private static final int CHUNK = 500000;
//
//    private final DataSource dataSource;
//    private final JdbcTemplate jdbcTemplate;
//
//    private final JobRepository jobRepository;
//    private final PlatformTransactionManager platformTransactionManager;
//
//    private final OrganizationService organizationService;
//    private final ProductAccountService productAccountService;
//    private final ServiceTypeService serviceTypeService;
//    private final JobStepService jobStepService;
//
//    @Bean
//    public Job mergeBillingDataJob() {
//
//        return new JobBuilder("mergeBillingDataJob", jobRepository)
//                .validator(parametersValidator())
//                .start(fetchAndStoreServiceTypeStep())
//                .next(fetchAndStoreAccountIdsForOrganizationStep())
//                .next(mergeHuaweiBillingDataStep())
//                .next(mergeGcpBillingDataStep())
//                .next(mergeAwsBillingDataStep())
//                .build();
//    }
//
//    private JobParametersValidator parametersValidator() {
//        return parameters -> {
//
//            if (parameters == null) {
//                throw new JobParametersInvalidException("Job parameters are required for mergeBillingDataJob");
//            }
//
//            Long orgId = parameters.getLong("orgId");
//
//            if (orgId == null) {
//                throw new JobParametersInvalidException("Organization ID is required for mergeBillingDataJob");
//            }
//
//        };
//    }
//
//    @Bean
//    public Step fetchAndStoreServiceTypeStep() {
//        return new StepBuilder("fetchAndStoreServiceTypeStep", jobRepository)
//                .tasklet((contribution, chunkContext) -> {
//
//                    serviceTypeService.fetchAndStoreServiceTypeMap();
//
//                    return RepeatStatus.FINISHED;
//                }, platformTransactionManager)
//                .build();
//    }
//
//    @Bean
//    public Step fetchAndStoreAccountIdsForOrganizationStep() {
//        return new StepBuilder("fetchAndStoreAccountIdsForOrganizationStep", jobRepository)
//                .tasklet((contribution, chunkContext) -> {
//
//                    Map<String, Object> parameters = chunkContext.getStepContext().getJobParameters();
//
//                    Long orgId = (Long) parameters.get("orgId");
//
//                    productAccountService.fetchAndStoreAccountIds(orgId);
//
//                    return RepeatStatus.FINISHED;
//                }, platformTransactionManager)
//                .build();
//    }
//
//    @Bean
//    public Step mergeHuaweiBillingDataStep() {
//
//        return new StepBuilder("mergeHuaweiBillingDataStep", jobRepository)
//                .<ServiceLevelBilling, ServiceLevelBilling>chunk(CHUNK, platformTransactionManager)
//                .reader(huaweiDataReader())
//                .processor(item -> {
//
//                    String parentCategory = serviceTypeService.getParentCategory(
//                            item.getServiceCode(), item.getCloudProvider()
//                    );
//
//                    item.setParentCategory(parentCategory);
//
//                    return item;
//                })
//                .writer(chunk -> {
//                    System.out.println(chunk.size());
//                })
////                .writer(this::upsertAll)
//                .build();
//    }
//
//    @Bean
//    @StepScope
//    public JdbcCursorItemReader<ServiceLevelBilling> huaweiDataReader() {
//
//        try {
//            return createReader(
//                    createSQL(
//                            ServiceLevelBillingSql.HUAWEI_SQL,
//                            productAccountService.getAccountIds(CloudProvider.HWC)
//                    ),
//                    "huaweiDataReader"
//            );
//        } catch (Exception e) {
//            throw new RuntimeException(e);
//        }
//
//    }
//
//    @Bean
//    public Step mergeGcpBillingDataStep() {
//        return new StepBuilder("mergeGcpBillingDataStep", jobRepository)
//                .<ServiceLevelBilling, ServiceLevelBilling>chunk(CHUNK, platformTransactionManager)
//                .reader(gcpDataReader())
//                .processor(item -> {
//
//                    String parentCategory = serviceTypeService.getParentCategory(
//                            item.getServiceCode(), item.getCloudProvider()
//                    );
//
//                    if (!StringUtils.hasText(parentCategory)) {
//
//                        if (item.getBillingType().equalsIgnoreCase("regular")) {
//                            item.setServiceCode("Unknown");
//                            item.setParentCategory("Unknown");
//                        }
//
//                    } else {
//                        item.setParentCategory(parentCategory);
//                    }
//
//                    return item;
//                })
//                .writer(chunk -> {
//                    System.out.println(chunk.size());
//                })
////                .writer(this::upsertAll)
//                .build();
//    }
//
//    @Bean
//    @StepScope
//    public JdbcCursorItemReader<ServiceLevelBilling> gcpDataReader() {
//
//        try {
//            return createReader(
//                    createSQL(
//                            ServiceLevelBillingSql.GCP_SQL,
//                            productAccountService.getAccountIds(CloudProvider.GCP)
//                    ),
//                    "gcpDataReader"
//            );
//        } catch (Exception e) {
//            throw new RuntimeException(e);
//        }
//
//    }
//
//    @Bean
//    public Step mergeAwsBillingDataStep() {
//        return new StepBuilder("mergeAwsBillingDataStep", jobRepository)
//                .<ServiceLevelBilling, ServiceLevelBilling>chunk(CHUNK, platformTransactionManager)
//                .reader(awsDataReader())
//                .processor(item -> {
//
//                    String parentCategory = serviceTypeService.getParentCategory(
//                            item.getServiceCode(), item.getCloudProvider()
//                    );
//
//                    if (!StringUtils.hasText(parentCategory)) {
//
//                        if (item.getBillingType().equalsIgnoreCase("Usage")) {
//                            item.setServiceCode("Unknown");
//                            item.setParentCategory("Unknown");
//                        }
//
//                    } else {
//                        item.setParentCategory(parentCategory);
//                    }
//
//                    return item;
//                })
//                .writer(chunk -> {
//                    System.out.println(chunk.size());
//                })
////                .writer(this::upsertAll)
//                .build();
//    }
//
//    @Bean
//    @StepScope
//    public JdbcCursorItemReader<ServiceLevelBilling> awsDataReader() {
//
//        try {
//            return createReader(
//                    createSQL(
//                            ServiceLevelBillingSql.AWS_SQL,
//                            productAccountService.getAccountIds(CloudProvider.AWS)
//                    ),
//                    "awsDataReader"
//            );
//        } catch (Exception e) {
//            throw new RuntimeException(e);
//        }
//
//    }
//
//    private JdbcCursorItemReader<ServiceLevelBilling> createReader(String sql,
//                                                                   String readerName) throws Exception {
//
//        LocalDate endDate = LocalDate.now();
//        LocalDate startDate = jobStepService.hasStepEverCompleted(readerName)
//                ? endDate.minusDays(7)
//                : LocalDate.parse("2025-01-01");
//
//        JdbcCursorItemReader<ServiceLevelBilling> reader = new JdbcCursorItemReader<>();
//        reader.setName(readerName);
//        reader.setDataSource(dataSource);
//        reader.setSql(sql);
//        reader.setFetchSize(0);
////        reader.setSaveState(false);
////        reader.setVerifyCursorPosition(false);
//
//        reader.setPreparedStatementSetter(ps -> {
//            ps.setObject(1, startDate);
//            ps.setObject(2, endDate);
//        });
//
//        reader.setRowMapper((rs, rowNum) -> ServiceLevelBilling.builder()
//                .usageDate(rs.getDate("usage_date").toLocalDate())
//                .cloudProvider(CloudProvider.valueOf(rs.getString("cloud_provider")))
//                .billingAccountId(rs.getString("billing_account_id"))
//                .usageAccountId(rs.getString("usage_account_id"))
//                .usageAccountName(rs.getString("usage_account_name"))
//                .serviceCode(rs.getString("service_code"))
//                .serviceName(rs.getString("service_name"))
//                .billingType(rs.getString("billing_type"))
//                .cost(rs.getBigDecimal("cost"))
//                .build());
//
//        // Must call this to ensure Spring can open the reader
//        reader.afterPropertiesSet();
//
//        return reader;
//    }
//
//    private void upsertAll(Chunk<? extends ServiceLevelBilling> records) {
//
//        if (records == null || records.isEmpty()) {
//            return;
//        }
//
//        log.info("Upserting {} records...", records.size());
//
//        jdbcTemplate.batchUpdate(
//                ServiceLevelBillingSql.UPSERT_SQL,
//                new BatchPreparedStatementSetter() {
//
//                    @Override
//                    public void setValues(PreparedStatement ps, int i) throws SQLException {
//
//                        ServiceLevelBilling item = records.getItems().get(i);
//
//                        ps.setDate(1, Date.valueOf(item.getUsageDate()));
//                        ps.setString(2, item.getCloudProvider().name());
//                        ps.setString(3, item.getBillingAccountId());
//                        ps.setString(4, item.getUsageAccountId());
//                        ps.setString(5, item.getUsageAccountName());
//                        ps.setString(6, item.getServiceCode());
//                        ps.setString(7, item.getServiceName());
//                        ps.setString(8, item.getBillingType());
//                        ps.setString(9, item.getParentCategory());
//                        ps.setBigDecimal(10, item.getCost());
//
//                    }
//
//                    @Override
//                    public int getBatchSize() {
//                        return records.size();
//                    }
//
//                }
//        );
//
//    }
//
//    private String createSQL(String sql, List<String> accountIds) {
//
//        String placeholders = accountIds.stream()
//                .map(v -> String.format("'%s'", v))
//                .collect(Collectors.joining(", "));
//
//        return sql.formatted(placeholders);
//    }
//
//}