package com.multicloud.batch.job.huawei;

import com.multicloud.batch.dao.huawei.HuaweiAuthService;
import com.multicloud.batch.dao.huawei.HuaweiBillingService;
import com.multicloud.batch.job.CustomDateRange;
import com.multicloud.batch.job.DateRangePartition;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.scope.context.StepSynchronizationManager;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
                .gridSize(5)
                .taskExecutor(huaweiBillingDataTaskExecutor())
                .build();
    }

    @Bean
    public SimpleAsyncTaskExecutor huaweiBillingDataTaskExecutor() {
        return new SimpleAsyncTaskExecutor();
    }

    @Bean
    public Partitioner huaweiBillingDataPartitioner() {

        return gridSize -> {

            StepExecution stepExecution = requireNonNull(StepSynchronizationManager.getContext()).getStepExecution();

            JobParameters jobParameters = stepExecution.getJobParameters();
            Long orgId = jobParameters.getLong("orgId");
            Long days = jobParameters.getLong("days");

            if (days == null || orgId == null || days < 1 || orgId < 1) {
                log.error("Invalid parameters for partition job...");
                return new HashMap<>();
            }

            List<CustomDateRange> dateRanges = DateRangePartition.getPartitions(days, 11);

            Map<String, ExecutionContext> partitions = new HashMap<>();

            int i = 1;

            for (CustomDateRange dateRange : dateRanges) {

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
    public Step huaweiBillingDataSlaveStep() {
        return new StepBuilder("huaweiBillingDataSlaveStep", jobRepository)
                .tasklet((contribution, chunkContext) -> {

                    StepExecution stepExecution = requireNonNull(StepSynchronizationManager.getContext()).getStepExecution();
                    CustomDateRange range = (CustomDateRange) stepExecution.getExecutionContext().get("range");
                    Long orgId = (Long) stepExecution.getExecutionContext().get("orgId");

                    if (range != null && orgId != null) {

                        log.info("Processing partition... {} - [{} - {}]", range.year(), range.start(), range.end());

                        String token = huaweiAuthService.login();
                        huaweiBillingService.fetchDailyServiceCostUsage(orgId, range, token);

                    }

                    return RepeatStatus.FINISHED;
                }, platformTransactionManager)
                .build();
    }

}
