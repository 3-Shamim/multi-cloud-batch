package com.multicloud.batch.job.huawei;

import com.multicloud.batch.dao.huawei.HuaweiAuthService;
import com.multicloud.batch.dao.huawei.HuaweiBillingService;
import com.multicloud.batch.enums.CloudProvider;
import com.multicloud.batch.enums.LastSyncStatus;
import com.multicloud.batch.job.CustomDateRange;
import com.multicloud.batch.job.DateRangePartition;
import com.multicloud.batch.model.DataSyncHistory;
import com.multicloud.batch.model.Organization;
import com.multicloud.batch.repository.DataSyncHistoryRepository;
import com.multicloud.batch.repository.OrganizationRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
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
public class HuaweiBillingDataJobConfig {

    private final JobRepository jobRepository;
    private final PlatformTransactionManager platformTransactionManager;

    private final OrganizationRepository organizationRepository;
    private final DataSyncHistoryRepository dataSyncHistoryRepository;
    private final HuaweiAuthService huaweiAuthService;
    private final HuaweiBillingService huaweiBillingService;

    @Bean
    public Job huaweiBillingDataJob() {
        return new JobBuilder("huaweiBillingDataJob", jobRepository)
                .start(huaweiLoginStep())
                .next(huaweiBillingDataMasterStep())
                .build();
    }

    @Bean
    public Step huaweiLoginStep() {
        return new StepBuilder("huaweiLoginStep", jobRepository)
                .tasklet((contribution, chunkContext) -> {
                    huaweiAuthService.login();
                    return RepeatStatus.FINISHED;
                }, platformTransactionManager)
                .build();
    }

    @Bean
    public Step huaweiBillingDataMasterStep() {
        return new StepBuilder("huaweiBillingDataMasterStep", jobRepository)
                .partitioner(huaweiBillingDataSlaveStep().getName(), huaweiBillingDataPartitioner())
                .step(huaweiBillingDataSlaveStep())
                .gridSize(1)
                .build();
    }

    @Bean
    public Partitioner huaweiBillingDataPartitioner() {

        return gridSize -> {

            StepExecution stepExecution = requireNonNull(StepSynchronizationManager.getContext()).getStepExecution();

            JobParameters jobParameters = stepExecution.getJobParameters();
            Long orgId = jobParameters.getLong("orgId");

            if (orgId == null || orgId < 1) {
                throw new RuntimeException("Invalid organization id...");
            }

            Organization org = organizationRepository.findById(orgId)
                    .orElseThrow(() -> new RuntimeException("Organization not found by ID: " + orgId));

            boolean exist = dataSyncHistoryRepository.existsAny(orgId, CloudProvider.HWC);

            long days = 365;

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
                executionContext.put("org", org);

                partitions.put("partition" + i, executionContext);

                i++;
            }

            List<DataSyncHistory> failList = dataSyncHistoryRepository.findAllByOrganizationIdAndCloudProviderAndLastSyncStatusAndFailCountLessThan(
                    orgId, CloudProvider.HWC, LastSyncStatus.FAIL, 3
            );

            for (DataSyncHistory item : failList) {

                CustomDateRange dateRange = new CustomDateRange(item.getStart(), item.getEnd(), item.getYear());

                if (unique.contains(dateRange)) {
                    continue;
                }

                unique.add(dateRange);

                ExecutionContext executionContext = new ExecutionContext();
                executionContext.put("range", dateRange);
                executionContext.put("org", org);

                partitions.put("partition" + i, executionContext);

                i++;
            }

            return partitions;
        };
    }

    @Bean
    public Step huaweiBillingDataSlaveStep() {
        return new StepBuilder("huaweiBillingDataSlaveStep", jobRepository)
                .tasklet((contribution, chunkContext) -> {

                    StepExecution stepExecution = requireNonNull(StepSynchronizationManager.getContext()).getStepExecution();
                    CustomDateRange range = (CustomDateRange) stepExecution.getExecutionContext().get("range");
                    Organization org = (Organization) stepExecution.getExecutionContext().get("org");

                    if (range != null && org != null) {

                        log.info(
                                "Processing huawei billing for partition {} and organization {}",
                                range, org.getId()
                        );

                        String token = huaweiAuthService.login();
                        huaweiBillingService.fetchDailyServiceCostUsage(
                                org.getId(), range, token
                        );

                    }

                    return RepeatStatus.FINISHED;
                }, platformTransactionManager)
                .listener(huaweiBillingDataStepListener())
                .build();
    }

    @Bean
    public StepExecutionListener huaweiBillingDataStepListener() {

        return new StepExecutionListener() {

            @Override
            public void beforeStep(@NotNull StepExecution stepExecution) {

                CustomDateRange range = (CustomDateRange) stepExecution.getExecutionContext().get("range");
                Organization org = (Organization) stepExecution.getExecutionContext().get("org");

                if (range != null && org != null) {

                    log.info(
                            "Starting step: {} for partition {} and organization {}",
                            stepExecution.getStepName(), range, org.getId()
                    );

                }
            }

            @Override
            public ExitStatus afterStep(@NotNull StepExecution stepExecution) {

                String partitionName = stepExecution.getStepName();
                BatchStatus status = stepExecution.getStatus();

                CustomDateRange range = (CustomDateRange) stepExecution.getExecutionContext().get("range");
                Organization org = (Organization) stepExecution.getExecutionContext().get("org");

                if (range != null && org != null) {

                    DataSyncHistory sync = dataSyncHistoryRepository.findByOrganizationIdAndCloudProviderAndJobNameAndStartAndEnd(
                            org.getId(), CloudProvider.HWC, "huaweiBillingDataJob", range.start(), range.end()
                    ).orElse(new DataSyncHistory(
                            org, CloudProvider.HWC, "huaweiBillingDataJob", range.start(), range.end()
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
                        "Step completed: {} with status: {} for partition {} and organization {}",
                        partitionName, status, range, org == null ? 0 : org.getId()
                );

                return stepExecution.getExitStatus();
            }
        };
    }

}
