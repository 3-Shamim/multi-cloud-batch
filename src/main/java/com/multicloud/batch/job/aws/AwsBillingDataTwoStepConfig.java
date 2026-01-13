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
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.scope.context.StepSynchronizationManager;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
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
public class AwsBillingDataTwoStepConfig {

    private static final String SECRET_STORE_KEY = "global_aws_billing_data_secret";

    private static final String DATABASE_NAME = "athenacurcfn_athena";

    @Value("${batch_job.aws_billing_data.secret_path}")
    private String awsSecretPath;

    private final JobRepository jobRepository;
    private final PlatformTransactionManager platformTransactionManager;

    private final AwsDataSyncHistoryRepository awsDataSyncHistoryRepository;
    private final SecretPayloadStoreService secretPayloadStoreService;
    private final AwsSecretsManagerService awsSecretsManagerService;
    private final AwsBillingService awsBillingService;

    @Bean
    public Step awsBillingDataTwoMasterStep() {
        return new StepBuilder("awsBillingDataTwoMasterStep", jobRepository)
                .partitioner(awsBillingDataTwoSlaveStep().getName(), awsBillingDataTwoPartitioner())
                .step(awsBillingDataTwoSlaveStep())
                .gridSize(1)
                .build();
    }

    @Bean
    public Partitioner awsBillingDataTwoPartitioner() {
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

            Set<String> tables = Set.of(
                    "cur-fr24data",
                    "cur-willhaben-data"
            );

            Set<AwsUniqueStep> unique = new HashSet<>();
            Map<String, ExecutionContext> partitions = new HashMap<>();

            int i = 1;

            for (String table : tables) {

                // Partition calculation
                boolean exist = awsDataSyncHistoryRepository.existsAny(AwsBillingDataJobConfig.JOB_NAME, table);

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

                List<CustomDateRange> dateRanges = DateRangePartition.getPartitions(days, 11);

                for (CustomDateRange dateRange : dateRanges) {

                    ExecutionContext executionContext = new ExecutionContext();
                    executionContext.put("range", dateRange);
                    executionContext.put("tableName", table);

                    partitions.put("partition" + i, executionContext);

                    unique.add(new AwsUniqueStep(table, dateRange));

                    i++;

                }

            }

            List<AwsDataSyncHistory> failList = awsDataSyncHistoryRepository.findAllByJobNameAndTableNameInAndLastSyncStatusAndFailCountLessThan(
                    AwsBillingDataJobConfig.JOB_NAME, tables, LastSyncStatus.FAIL, 3
            );

            for (AwsDataSyncHistory item : failList) {

                CustomDateRange dateRange = new CustomDateRange(
                        item.getStart(), item.getEnd(), item.getEnd().getYear(), item.getEnd().getMonthValue()
                );

                AwsUniqueStep awsUniqueStep = new AwsUniqueStep(item.getTableName(), dateRange);

                if (unique.contains(awsUniqueStep)) {
                    continue;
                }

                unique.add(new AwsUniqueStep(item.getTableName(), dateRange));

                ExecutionContext executionContext = new ExecutionContext();
                executionContext.put("range", dateRange);
                executionContext.put("tableName", item.getTableName());

                partitions.put("partition" + i, executionContext);

                i++;
            }

            return partitions;
        };
    }

    @Bean
    public Step awsBillingDataTwoSlaveStep() {
        return new StepBuilder("awsBillingDataTwoSlaveStep", jobRepository)
                .tasklet((contribution, chunkContext) -> {

                    StepExecution stepExecution = requireNonNull(
                            StepSynchronizationManager.getContext()
                    ).getStepExecution();

                    CustomDateRange range = (CustomDateRange) stepExecution.getExecutionContext().get("range");
                    String tableName = (String) stepExecution.getExecutionContext().get("tableName");

                    if (range != null && tableName != null) {

                        log.info("Processing partition {} for awsBillingDataJob", range);

                        SecretPayload secret = secretPayloadStoreService.get(SECRET_STORE_KEY);

                        awsBillingService.syncDailyCostUsageFromAthenaV2(
                                DATABASE_NAME, tableName,
                                secret.getAccessKey(), secret.getSecretKey(), secret.getRegion(),
                                range.start(), range.end(), true
                        );

                    }

                    return RepeatStatus.FINISHED;
                }, platformTransactionManager)
                .listener(awsBillingDataTwoStepListener())
                .build();
    }

    @Bean
    public StepExecutionListener awsBillingDataTwoStepListener() {

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
                String tableName = (String) stepExecution.getExecutionContext().get("tableName");

                if (range != null && tableName != null) {

                    AwsDataSyncHistory sync = awsDataSyncHistoryRepository.findByJobNameAndTableNameAndStartAndEnd(
                            AwsBillingDataJobConfig.JOB_NAME, tableName, range.start(), range.end()
                    ).orElse(new AwsDataSyncHistory(
                            AwsBillingDataJobConfig.JOB_NAME, tableName, range.start(), range.end()
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