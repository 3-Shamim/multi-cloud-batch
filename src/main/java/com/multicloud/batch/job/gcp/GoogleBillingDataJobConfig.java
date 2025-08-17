package com.multicloud.batch.job.gcp;

import com.multicloud.batch.dao.aws.AwsSecretsManagerService;
import com.multicloud.batch.dao.aws.payload.SecretPayload;
import com.multicloud.batch.dao.google.GoogleBillingService;
import com.multicloud.batch.enums.CloudProvider;
import com.multicloud.batch.enums.LastSyncStatus;
import com.multicloud.batch.job.CustomDateRange;
import com.multicloud.batch.job.DateRangePartition;
import com.multicloud.batch.model.DataSyncHistory;
import com.multicloud.batch.repository.DataSyncHistoryRepository;
import com.multicloud.batch.service.CloudConfigService;
import com.multicloud.batch.service.SecretPayloadStoreService;
import com.multicloud.batch.util.Util;
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
public class GoogleBillingDataJobConfig {

    private final JobRepository jobRepository;
    private final PlatformTransactionManager platformTransactionManager;

    private final DataSyncHistoryRepository dataSyncHistoryRepository;
    private final CloudConfigService cloudConfigService;
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

            StepExecution stepExecution = requireNonNull(
                    StepSynchronizationManager.getContext()
            ).getStepExecution();

            JobParameters jobParameters = stepExecution.getJobParameters();
            Long orgId = jobParameters.getLong("orgId");

            if (orgId == null || orgId < 1) {
                throw new RuntimeException("Invalid organization ID...");
            }

            Optional<String> secretARN = cloudConfigService.getConfigByOrganizationIdAndCloudProvider(
                    orgId, CloudProvider.GCP
            );

            if (secretARN.isEmpty()) {
                throw new RuntimeException("GCP config not found for organization ID: " + orgId);
            }

            SecretPayload secret = awsSecretsManagerService.getSecret(secretARN.get());

            if (secret == null) {
                throw new RuntimeException("GCP secret not found for organization ID: " + orgId);
            }

            // Store secret
            secretPayloadStoreService.put(Util.getProviderStoreKey(orgId, CloudProvider.GCP), secret);

            // Partition calculation
            boolean exist = dataSyncHistoryRepository.existsAny(orgId, CloudProvider.GCP);

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
                executionContext.put("orgId", orgId);

                partitions.put("partition" + i, executionContext);

                i++;
            }

            List<DataSyncHistory> failList = dataSyncHistoryRepository.findAllByOrganizationIdAndCloudProviderAndLastSyncStatusAndFailCountLessThan(
                    orgId, CloudProvider.GCP, LastSyncStatus.FAIL, 5
            );

            for (DataSyncHistory item : failList) {

                CustomDateRange dateRange = new CustomDateRange(
                        item.getStart(), item.getEnd(), item.getEnd().getYear(), item.getEnd().getMonthValue()
                );

                if (unique.contains(dateRange)) {
                    continue;
                }

                unique.add(dateRange);

                ExecutionContext executionContext = new ExecutionContext();
                executionContext.put("range", dateRange);
                executionContext.put("orgId", orgId);

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
                    Long orgId = (Long) stepExecution.getExecutionContext().get("orgId");

                    if (range != null && orgId != null) {

                        log.info("Processing gcp billing for partition {} and organization ID {}", range, orgId);

                        SecretPayload secret = secretPayloadStoreService.get(
                                Util.getProviderStoreKey(orgId, CloudProvider.GCP)
                        );

                        googleBillingService.fetchDailyServiceCostUsage(
                                orgId, secret.getJsonKey().getBytes(), range.start(), range.end()
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
                Long orgId = (Long) stepExecution.getExecutionContext().get("orgId");

                if (range != null && orgId != null) {

                    log.info(
                            "Starting step: {} for partition {} and organization ID {}",
                            stepExecution.getStepName(), range, orgId
                    );

                }
            }

            @Override
            public ExitStatus afterStep(StepExecution stepExecution) {

                String partitionName = stepExecution.getStepName();
                BatchStatus status = stepExecution.getStatus();

                if (!stepExecution.getFailureExceptions().isEmpty()) {
                    stepExecution.getFailureExceptions()
                            .forEach(ex -> log.error(
                                    "Exception in step {}: ", partitionName, ex
                            ));
                }

                CustomDateRange range = (CustomDateRange) stepExecution.getExecutionContext().get("range");
                Long orgId = (Long) stepExecution.getExecutionContext().get("orgId");

                if (range != null && orgId != null) {

                    DataSyncHistory sync = dataSyncHistoryRepository.findByOrganizationIdAndCloudProviderAndJobNameAndStartAndEnd(
                            orgId, CloudProvider.GCP, "gcpBillingDataJob", range.start(), range.end()
                    ).orElse(new DataSyncHistory(
                            orgId, CloudProvider.GCP, "gcpBillingDataJob", range.start(), range.end()
                    ));

                    if (status.equals(BatchStatus.COMPLETED)) {
                        sync.setLastSyncStatus(LastSyncStatus.SUCCESS);
                    } else {
                        sync.setLastSyncStatus(LastSyncStatus.FAIL);
                        sync.setFailCount(sync.getFailCount() + 1);
                    }

                    dataSyncHistoryRepository.save(sync);

                }

                log.info(
                        "Step completed: {} with status: {} for partition {} and organization ID {}",
                        partitionName, status, range, orgId
                );

                return stepExecution.getExitStatus();
            }
        };
    }

}
