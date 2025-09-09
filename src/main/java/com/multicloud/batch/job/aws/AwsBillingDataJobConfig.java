package com.multicloud.batch.job.aws;

import com.multicloud.batch.dao.aws.AwsBillingService;
import com.multicloud.batch.dao.aws.AwsSecretsManagerService;
import com.multicloud.batch.dao.aws.payload.SecretPayload;
import com.multicloud.batch.dto.CloudConfigDTO;
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
@ConditionalOnProperty(name = "batch_job.aws_billing_data.enabled", havingValue = "true")
public class AwsBillingDataJobConfig {

    private final JobRepository jobRepository;
    private final PlatformTransactionManager platformTransactionManager;

    private final DataSyncHistoryRepository dataSyncHistoryRepository;
    private final CloudConfigService cloudConfigService;
    private final SecretPayloadStoreService secretPayloadStoreService;
    private final AwsSecretsManagerService awsSecretsManagerService;
    private final AwsBillingService awsBillingService;

    @Bean
    public Job awsBillingDataJob() {
        return new JobBuilder("awsBillingDataJob", jobRepository)
                .start(awsBillingDataMasterStep())
                .build();
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

            StepExecution stepExecution = requireNonNull(
                    StepSynchronizationManager.getContext()
            ).getStepExecution();

            JobParameters jobParameters = stepExecution.getJobParameters();
            Long orgId = jobParameters.getLong("orgId");

            if (orgId == null || orgId < 1) {
                throw new RuntimeException("Invalid organization ID...");
            }

            Optional<CloudConfigDTO> cloudConfig = cloudConfigService.getConfigByOrganizationIdAndCloudProvider(
                    orgId, CloudProvider.AWS
            );

            if (cloudConfig.isEmpty()) {
                throw new RuntimeException("AWS config not found for organization ID: " + orgId);
            }

            if (cloudConfig.get().disabled()) {
                throw new RuntimeException("AWS config is disabled for organization ID: " + orgId);
            }

            SecretPayload secret = awsSecretsManagerService.getSecret(cloudConfig.get().secretARN());

            if (secret == null) {
                throw new RuntimeException("AWS secret not found for organization ID: " + orgId);
            }

            // Store secret
            secretPayloadStoreService.put(Util.getProviderStoreKey(orgId, CloudProvider.AWS), secret);

            // Partition calculation
            boolean exist = dataSyncHistoryRepository.existsAny(orgId, CloudProvider.AWS);

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
                    orgId, CloudProvider.AWS, LastSyncStatus.FAIL, 5
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
    public Step awsBillingDataSlaveStep() {
        return new StepBuilder("awsBillingDataSlaveStep", jobRepository)
                .tasklet((contribution, chunkContext) -> {

                    StepExecution stepExecution = requireNonNull(
                            StepSynchronizationManager.getContext()
                    ).getStepExecution();

                    CustomDateRange range = (CustomDateRange) stepExecution.getExecutionContext().get("range");
                    Long orgId = (Long) stepExecution.getExecutionContext().get("orgId");

                    if (range != null && orgId != null) {

                        log.info("Processing aws billing for partition {} and organization ID {}", range, orgId);

                        SecretPayload secret = secretPayloadStoreService.get(
                                Util.getProviderStoreKey(orgId, CloudProvider.AWS)
                        );

                        awsBillingService.syncDailyCostUsageFromAthena(
                                orgId, secret.getAccessKey(), secret.getSecretKey(), secret.getRegion(),
                                range.start(), range.end()
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
                            orgId, CloudProvider.AWS, "awsBillingDataJob", range.start(), range.end()
                    ).orElse(new DataSyncHistory(
                            orgId, CloudProvider.AWS, "awsBillingDataJob", range.start(), range.end()
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
