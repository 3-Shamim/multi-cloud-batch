package com.multicloud.batch.job;

import com.multicloud.batch.enums.CloudProvider;
import com.multicloud.batch.helper.ServiceLevelBillingSql;
import com.multicloud.batch.model.ServiceLevelBilling;
import com.multicloud.batch.service.JobStepService;
import com.multicloud.batch.service.OrganizationService;
import com.multicloud.batch.service.ProductAccountService;
import com.multicloud.batch.service.ServiceTypeService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.scope.context.StepSynchronizationManager;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.SingleColumnRowMapper;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.io.Serializable;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim Molla
 * Email: shamim.molla@vivasoftltd.com
 */

@Slf4j
@Configuration
@RequiredArgsConstructor
@ConditionalOnExpression("${batch_job.merge_billing.enabled}")
public class MergeBillingDataJobConfig {

    private static final int CHUNK = 500000;

    private final DataSource dataSource;
    private final JdbcTemplate jdbcTemplate;

    private final JobRepository jobRepository;
    private final PlatformTransactionManager platformTransactionManager;

    private final OrganizationService organizationService;
    private final ProductAccountService productAccountService;
    private final ServiceTypeService serviceTypeService;
    private final JobStepService jobStepService;

    private final OrganizationIdValidator organizationIdValidator;
    private final Step cacheServiceTypeStep;

    @Bean
    public Job mergeBillingDataJob(Step masterStep) {

        return new JobBuilder("mergeBillingDataJob", jobRepository)
                .validator(organizationIdValidator)
                .start(cacheServiceTypeStep)
                .next(masterStep)
                .build();
    }

    @Bean
    public Step masterStep(Step workerStep) {

        return new StepBuilder("masterStep", jobRepository)
                .partitioner(workerStep.getName(), partitioner())
                .step(workerStep)
                .gridSize(1)
                .build();
    }

    @Bean
    public Partitioner partitioner() {

        return grid -> {

            StepExecution stepExecution = requireNonNull(
                    StepSynchronizationManager.getContext()
            ).getStepExecution();

            Long orgId = stepExecution.getJobParameters().getLong("orgId");

            if (orgId == null) {
                throw new RuntimeException("Organization ID is not set for mergeBillingDataJob");
            }

            Map<String, ExecutionContext> partitions = new HashMap<>();

            List<String> allAccountIds = productAccountService.findAccountIds(orgId, CloudProvider.AWS);

            int partitionSize = 150;
            int partitionNumber = 0;

            for (int i = 0; i < allAccountIds.size(); i += partitionSize) {

                List<String> subList = allAccountIds.subList(i, Math.min(i + partitionSize, allAccountIds.size()));

                ExecutionContext context = new ExecutionContext();
                context.put("accountIds", new AccountIds(new ArrayList<>(subList)));

                partitions.put("partition" + partitionNumber, context);

                partitionNumber++;

            }

            return partitions;
        };
    }

    @Bean
    public Step workerStep(ItemReader<ServiceLevelBilling> awsReader) {

        return new StepBuilder("workerStep", jobRepository)
                .<ServiceLevelBilling, ServiceLevelBilling>chunk(5000, platformTransactionManager)
                .reader(awsReader) // Step-scoped reader, accountIds injected
                .processor(item -> {

                    return item;
                })
                .writer(chunk -> {
                    System.out.println(chunk.size());
                })
                .build();
    }

    @Bean
    @StepScope
    public ItemReader<ServiceLevelBilling> awsReader() throws Exception {

        StepExecution stepExecution = requireNonNull(
                StepSynchronizationManager.getContext()
        ).getStepExecution();

        AccountIds accountIds = (AccountIds) stepExecution.getExecutionContext().get("accountIds");

        if (accountIds == null) {
            throw new RuntimeException("Account IDs are not set for workerStep");
        }

        LocalDate endDate = LocalDate.now();
        LocalDate startDate = jobStepService.hasStepEverCompleted("workerStep")
                ? endDate.minusDays(7)
                : LocalDate.parse("2025-01-01");

        JdbcCursorItemReader<ServiceLevelBilling> reader = new JdbcCursorItemReader<>();
//        reader.setName("workerStep");
        reader.setDataSource(dataSource);
        reader.setSql(createSQL(accountIds.accountIds));
        reader.setFetchSize(0);
        reader.setSaveState(false);
        reader.setVerifyCursorPosition(false);

        reader.setPreparedStatementSetter(ps -> {
            ps.setObject(1, startDate);
            ps.setObject(2, endDate);
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
                .cost(rs.getBigDecimal("cost"))
                .build());

        // Must call this to ensure Spring can open the reader
        reader.afterPropertiesSet();

        reader.open(new ExecutionContext());

        return reader;
    }

    private String createSQL(List<String> accountIds) {

        String placeholders = accountIds.stream()
                .map(v -> String.format("'%s'", v))
                .collect(Collectors.joining(", "));

        return ServiceLevelBillingSql.AWS_SQL.formatted(placeholders);
    }

    record AccountIds(List<String> accountIds) implements Serializable {
    }

}