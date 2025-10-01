package com.multicloud.batch.job.gcp;

import com.multicloud.batch.dao.aws.AwsSecretsManagerService;
import com.multicloud.batch.dao.aws.payload.SecretPayload;
import com.multicloud.batch.dao.google.GoogleBillingService;
import com.multicloud.batch.enums.LastSyncStatus;
import com.multicloud.batch.job.CustomDateRange;
import com.multicloud.batch.job.DateRangePartition;
import com.multicloud.batch.model.GcpDataSyncHistory;
import com.multicloud.batch.repository.GcpDataSyncHistoryRepository;
import com.multicloud.batch.service.SecretPayloadStoreService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.*;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.scope.context.StepSynchronizationManager;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
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
@ConditionalOnProperty(name = "batch_job.gcp_billing_data.enabled", havingValue = "true")
public class GoogleBillingDataJobConfig {

    private static final String JOB_NAME = "gcpBillingDataJob";
    private static final String SECRET_STORE_KEY = "gcp_internal_billing_data_secret";

    private static final String PROJECT_NAME = "internal_project";

    @Value("${batch_job.gcp_billing_data.secret_path}")
    private String gcpInternalSecretPath;

    private final JobRepository jobRepository;
    private final PlatformTransactionManager platformTransactionManager;

    private final GcpDataSyncHistoryRepository gcpDataSyncHistoryRepository;
    private final SecretPayloadStoreService secretPayloadStoreService;
    private final AwsSecretsManagerService awsSecretsManagerService;
    private final GoogleBillingService googleBillingService;

    @Bean
    public Job gcpBillingDataJob() {
        return new JobBuilder("gcpBillingDataJob", jobRepository)
                .start(gcpBillingDataMasterStep())
                .build();
    }

    // Using a single thread and fetched 3-day of data at once.
    // There is a lot of data aiming to store it slowly.
    @Bean
    public Step gcpBillingDataMasterStep() {
        return new StepBuilder("gcpBillingDataMasterStep", jobRepository)
                .partitioner(gcpBillingDataSlaveStep().getName(), gcpBillingDataPartitioner())
                .step(gcpBillingDataSlaveStep())
                .gridSize(1)
                .build();
    }

    @Bean
    public Partitioner gcpBillingDataPartitioner() {

        return gridSize -> {

            SecretPayload secret = secretPayloadStoreService.get(SECRET_STORE_KEY);

            if (secret == null) {

                secret = awsSecretsManagerService.getSecret(gcpInternalSecretPath, true);

                if (secret == null) {
                    throw new RuntimeException("Secret not found for gcpBillingDataJob");
                }

                // Store secret
                secretPayloadStoreService.put(SECRET_STORE_KEY, secret);

            }

            // Partition calculation
            boolean exist = gcpDataSyncHistoryRepository.existsAny(JOB_NAME);

            long days = ChronoUnit.DAYS.between(
                    LocalDate.parse("2025-01-01"), LocalDate.now()
            ) + 1;

            if (exist) {
                days = 7;
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

            List<GcpDataSyncHistory> failList = gcpDataSyncHistoryRepository.findAllByJobNameAndProjectAndLastSyncStatusAndFailCountLessThan(
                    JOB_NAME, PROJECT_NAME, LastSyncStatus.FAIL, 3
            );

            for (GcpDataSyncHistory item : failList) {

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
    public Step gcpBillingDataSlaveStep() {
        return new StepBuilder("gcpBillingDataSlaveStep", jobRepository)
                .tasklet((contribution, chunkContext) -> {

                    StepExecution stepExecution = requireNonNull(
                            StepSynchronizationManager.getContext()
                    ).getStepExecution();

                    CustomDateRange range = (CustomDateRange) stepExecution.getExecutionContext().get("range");

                    if (range != null) {

                        log.info("Processing partition {} for gcpBillingDataJob", range);

                        SecretPayload secret = secretPayloadStoreService.get(SECRET_STORE_KEY);

                        googleBillingService.fetchDailyServiceCostUsage(
                                secret.getJsonKey().getBytes(), range.start(), range.end()
                        );

                    }

                    return RepeatStatus.FINISHED;
                }, platformTransactionManager)
                .listener(gcpBillingDataStepListener())
                .build();
    }

    @Bean
    public StepExecutionListener gcpBillingDataStepListener() {

        return new StepExecutionListener() {

            @Override
            public void beforeStep(StepExecution stepExecution) {

                CustomDateRange range = (CustomDateRange) stepExecution.getExecutionContext().get("range");

                if (range != null) {

                    log.info("Starting gcpBillingDataJob's step: {} for partition {}", stepExecution.getStepName(), range);

                }
            }

            @Override
            public ExitStatus afterStep(StepExecution stepExecution) {

                String partitionName = stepExecution.getStepName();
                BatchStatus status = stepExecution.getStatus();

                if (!stepExecution.getFailureExceptions().isEmpty()) {
                    stepExecution.getFailureExceptions()
                            .forEach(ex -> log.error(
                                    "GcpBillingDataJob exception in step {}: ", partitionName, ex
                            ));
                }

                CustomDateRange range = (CustomDateRange) stepExecution.getExecutionContext().get("range");

                if (range != null) {

                    GcpDataSyncHistory sync = gcpDataSyncHistoryRepository.findByJobNameAndProjectAndStartAndEnd(
                            JOB_NAME, PROJECT_NAME, range.start(), range.end()
                    ).orElse(new GcpDataSyncHistory(
                            JOB_NAME, PROJECT_NAME, range.start(), range.end()
                    ));

                    if (status.equals(BatchStatus.COMPLETED)) {
                        sync.setLastSyncStatus(LastSyncStatus.SUCCESS);
                    } else {
                        sync.setLastSyncStatus(LastSyncStatus.FAIL);
                        sync.setFailCount(sync.getFailCount() + 1);
                    }

                    gcpDataSyncHistoryRepository.save(sync);

                }

                log.info(
                        "GcpBillingDataJob's step completed: {} with status: {} for partition {}",
                        partitionName, status, range
                );

                return stepExecution.getExitStatus();
            }
        };
    }

}
