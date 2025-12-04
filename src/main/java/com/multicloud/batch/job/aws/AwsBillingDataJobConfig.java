package com.multicloud.batch.job.aws;

import com.multicloud.batch.dao.aws.AwsBillingService;
import com.multicloud.batch.dao.aws.AwsSecretsManagerService;
import com.multicloud.batch.dao.aws.payload.SecretPayload;
import com.multicloud.batch.enums.LastSyncStatus;
import com.multicloud.batch.job.CustomDateRange;
import com.multicloud.batch.job.DateRangePartition;
import com.multicloud.batch.model.AwsDataSyncHistory;
import com.multicloud.batch.repository.AwsDataSyncHistoryRepository;
import com.multicloud.batch.service.SecretPayloadStoreService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.*;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.scope.context.StepSynchronizationManager;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;

import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.*;

import static java.util.Objects.requireNonNull;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim Molla
 * Email: shamim.molla@vivasoftltd.com
 */

@Slf4j
@Configuration
@RequiredArgsConstructor
@ConditionalOnExpression("${batch_job.aws_billing_data.enabled}")
public class AwsBillingDataJobConfig {

    private static final String JOB_NAME = "awsBillingDataJob";
    private static final String SECRET_STORE_KEY = "global_aws_billing_data_secret";

    private static final String DATABASE_NAME = "athenacurcfn_athena";
    private static final String TABLE_NAME = "athena";

    @Value("${batch_job.aws_billing_data.secret_path}")
    private String awsSecretPath;

    private final JobRepository jobRepository;
    private final PlatformTransactionManager platformTransactionManager;

    private final AwsDataSyncHistoryRepository awsDataSyncHistoryRepository;
    private final SecretPayloadStoreService secretPayloadStoreService;
    private final AwsSecretsManagerService awsSecretsManagerService;
    private final AwsBillingService awsBillingService;

    private final JdbcTemplate jdbcTemplate;

    @Bean
    public Job awsBillingDataJob() {
        return new JobBuilder(JOB_NAME, jobRepository)
                .start(lastMonthAwsDataCleanupStep())
                .next(awsBillingDataMasterStep())
                .build();
    }

    // This will remove all AWS data based on date filter
    // We don't need to add this step in all AWS jobs
    @Bean
    public Step lastMonthAwsDataCleanupStep() {

        return new StepBuilder("lastMonthAwsDataCleanupStep", jobRepository)
                .tasklet((contribution, chunkContext) -> {

                    LocalDate now = LocalDate.now();

                    if (Set.of(1, 2, 3, 4).contains(now.getDayOfMonth())) {

                        String query = "delete from aws_billing_daily_costs where usage_date >= ?";

                        jdbcTemplate.update(query, now.minusMonths(1).withDayOfMonth(1));

                    }

                    return RepeatStatus.FINISHED;
                }, platformTransactionManager).build();
    }

    // Using a single thread and fetched 3-day of data at once.
    // There is a lot of data aiming to store it slowly.
    @Bean
    public Step awsBillingDataMasterStep() {
        return new StepBuilder("awsBillingDataMasterStep", jobRepository)
                .partitioner(awsBillingDataSlaveStep().getName(), awsBillingDataPartitioner())
                .step(awsBillingDataSlaveStep())
                .gridSize(1)
                .build();
    }

    @Bean
    public Partitioner awsBillingDataPartitioner() {

        return gridSize -> {

            StepExecution stepExecution = requireNonNull(StepSynchronizationManager.getContext()).getStepExecution();

            JobParameters jobParameters = stepExecution.getJobParameters();
            LocalDate startDate = jobParameters.getLocalDate("startDate");

            SecretPayload secret = secretPayloadStoreService.get(SECRET_STORE_KEY);

            if (secret == null) {

                secret = awsSecretsManagerService.getSecret(awsSecretPath, true);

                if (secret == null) {
                    throw new RuntimeException("Secret not found for awsBillingDataJob");
                }

                // Store secret
                secretPayloadStoreService.put(SECRET_STORE_KEY, secret);

            }

            // Partition calculation
            boolean exist = awsDataSyncHistoryRepository.existsAny(JOB_NAME, TABLE_NAME);

            LocalDate now = LocalDate.now();

            long days = ChronoUnit.DAYS.between(
                    now.minusMonths(6).withDayOfMonth(1), now
            ) + 1;

            if (exist) {

                if (Set.of(1, 2, 3, 4).contains(now.getDayOfMonth())) {

                    days = ChronoUnit.DAYS.between(
                            now.minusMonths(1).withDayOfMonth(1), now
                    ) + 1;

                } else {
                    days = Math.min(now.getDayOfMonth(), 10);
                }

            }

            if (startDate != null) {
                days = ChronoUnit.DAYS.between(
                        startDate, now
                ) + 1;
            }

            List<CustomDateRange> dateRanges = DateRangePartition.getPartitions(days, 3);

            Set<CustomDateRange> unique = new HashSet<>();
            Map<String, ExecutionContext> partitions = new HashMap<>();

            int i = 1;

            for (CustomDateRange dateRange : dateRanges) {

                ExecutionContext executionContext = new ExecutionContext();
                executionContext.put("range", dateRange);

                partitions.put("partition" + i, executionContext);

                unique.add(dateRange);

                i++;

            }

            List<AwsDataSyncHistory> failList = awsDataSyncHistoryRepository.findAllByJobNameAndTableNameAndLastSyncStatusAndFailCountLessThan(
                    JOB_NAME, TABLE_NAME, LastSyncStatus.FAIL, 3
            );

            for (AwsDataSyncHistory item : failList) {

                CustomDateRange dateRange = new CustomDateRange(
                        item.getStart(), item.getEnd(), item.getEnd().getYear(), item.getEnd().getMonthValue()
                );

                if (unique.contains(dateRange)) {
                    continue;
                }

                unique.add(dateRange);

                ExecutionContext executionContext = new ExecutionContext();
                executionContext.put("range", dateRange);

                partitions.put("partition" + i, executionContext);

                i++;
            }

            return partitions;
        };
    }

    @Bean
    public Step awsBillingDataSlaveStep() {
        return new StepBuilder("awsBillingDataSlaveStep", jobRepository)
                .tasklet((contribution, chunkContext) -> {

                    StepExecution stepExecution = requireNonNull(
                            StepSynchronizationManager.getContext()
                    ).getStepExecution();

                    CustomDateRange range = (CustomDateRange) stepExecution.getExecutionContext().get("range");

                    if (range != null) {

                        log.info("Processing partition {} for awsBillingDataJob", range);

                        SecretPayload secret = secretPayloadStoreService.get(SECRET_STORE_KEY);

                        awsBillingService.syncDailyCostUsageFromAthenaTable(
                                DATABASE_NAME, TABLE_NAME,
                                secret.getAccessKey(), secret.getSecretKey(), secret.getRegion(),
                                range.start(), range.end(), true
                        );

                    }

                    return RepeatStatus.FINISHED;
                }, platformTransactionManager)
                .listener(awsBillingDataStepListener())
                .build();
    }

    @Bean
    public StepExecutionListener awsBillingDataStepListener() {

        return new StepExecutionListener() {

            @Override
            public void beforeStep(StepExecution stepExecution) {

                CustomDateRange range = (CustomDateRange) stepExecution.getExecutionContext().get("range");

                if (range != null) {
                    log.info(
                            "Starting awsBillingDataJob's step: {} for partition {}",
                            stepExecution.getStepName(), range
                    );
                }

            }

            @Override
            public ExitStatus afterStep(StepExecution stepExecution) {

                String partitionName = stepExecution.getStepName();
                BatchStatus status = stepExecution.getStatus();

                if (!stepExecution.getFailureExceptions().isEmpty()) {

                    for (Throwable ex : stepExecution.getFailureExceptions()) {
                        log.error("AwsBillingDataJob exception in step {}: ", partitionName, ex);
                    }

                }

                CustomDateRange range = (CustomDateRange) stepExecution.getExecutionContext().get("range");

                if (range != null) {

                    AwsDataSyncHistory sync = awsDataSyncHistoryRepository.findByJobNameAndTableNameAndStartAndEnd(
                            JOB_NAME, TABLE_NAME, range.start(), range.end()
                    ).orElse(new AwsDataSyncHistory(
                            JOB_NAME, TABLE_NAME, range.start(), range.end()
                    ));

                    if (status.equals(BatchStatus.COMPLETED)) {
                        sync.setLastSyncStatus(LastSyncStatus.SUCCESS);
                    } else {
                        sync.setLastSyncStatus(LastSyncStatus.FAIL);
                        sync.setFailCount(sync.getFailCount() + 1);
                    }

                    awsDataSyncHistoryRepository.save(sync);

                }

                log.info(
                        "AwsBillingDataJob's step completed: {} with status: {} for partition {}",
                        partitionName, status, range
                );

                return stepExecution.getExitStatus();
            }
        };
    }

}
