package com.multicloud.batch.job.huawei;

import com.multicloud.batch.dao.aws.AwsSecretsManagerService;
import com.multicloud.batch.dao.aws.payload.SecretPayload;
import com.multicloud.batch.dao.huawei.HuaweiAuthService;
import com.multicloud.batch.dao.huawei.HuaweiBillingService;
import com.multicloud.batch.dao.huawei.payload.HuaweiAuthDetails;
import com.multicloud.batch.enums.LastSyncStatus;
import com.multicloud.batch.job.CustomDateRange;
import com.multicloud.batch.job.DateRangePartition;
import com.multicloud.batch.model.HuaweiDataSyncHistory;
import com.multicloud.batch.repository.HuaweiDataSyncHistoryRepository;
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
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
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
@ConditionalOnExpression("${batch_job.external_aws_billing_data.enabled}")
public class ExternalHuaweiBillingDataJobConfig {

    private static final String JOB_NAME = "externalHuaweiBillingDataJob";
    private static final String SECRET_STORE_KEY = "huawei_external_billing_data_secret";

    @Value("${batch_job.external_aws_billing_data.secret_path}")
    private String huaweiExternalSecretPath;

    private final JobRepository jobRepository;
    private final PlatformTransactionManager platformTransactionManager;

    private final HuaweiDataSyncHistoryRepository huaweiDataSyncHistoryRepository;
    private final AwsSecretsManagerService awsSecretsManagerService;
    private final SecretPayloadStoreService secretPayloadStoreService;
    private final HuaweiAuthService huaweiAuthService;
    private final HuaweiBillingService huaweiBillingService;

    @Bean
    public Job externalHuaweiBillingDataJob() {
        return new JobBuilder("externalHuaweiBillingDataJob", jobRepository)
                .start(huaweiLoginStep())
//                .next(externalHuaweiBillingDataMasterStep())
                .build();
    }

    @Bean
    public Step huaweiLoginStep() {
        return new StepBuilder("huaweiLoginStep", jobRepository)
                .tasklet((contribution, chunkContext) -> {

                    SecretPayload secret = secretPayloadStoreService.get(SECRET_STORE_KEY);

                    if (secret == null) {

                        secret = awsSecretsManagerService.getSecret(huaweiExternalSecretPath, true);

                        if (secret == null) {
                            throw new RuntimeException("Secret not found for externalHuaweiBillingDataJob");
                        }

                        // Store secret
                        secretPayloadStoreService.put(SECRET_STORE_KEY, secret);

                    }

                    // Region is the default project name
                    // Login
                    huaweiAuthService.login(
                            secret.getUsername(), secret.getPassword(), secret.getDomainName(), secret.getRegion()
                    );

                    return RepeatStatus.FINISHED;
                }, platformTransactionManager)
                .build();
    }

//    @Bean
//    public Step externalHuaweiBillingDataMasterStep() {
//        return new StepBuilder("externalHuaweiBillingDataMasterStep", jobRepository)
//                .partitioner(externalHuaweiBillingDataSlaveStep().getName(), externalHuaweiBillingDataPartitioner())
//                .step(externalHuaweiBillingDataSlaveStep())
//                .gridSize(1)
//                .build();
//    }

//    @Bean
//    public Partitioner externalHuaweiBillingDataPartitioner() {
//
//        return gridSize -> {
//
//            // Partition calculation
//            boolean exist = huaweiDataSyncHistoryRepository.existsAny(JOB_NAME);
//
//            long days = ChronoUnit.DAYS.between(
//                    LocalDate.parse("2025-01-01"), LocalDate.now()
//            ) + 1;
//
//            if (exist) {
//                days = 7;
//            }
//
//            List<CustomDateRange> dateRanges = DateRangePartition.getPartitions(days, 3);
//
//            Set<CustomDateRange> unique = new HashSet<>();
//            Map<String, ExecutionContext> partitions = new HashMap<>();
//
//            int i = 1;
//
//            for (CustomDateRange dateRange : dateRanges) {
//
//                ExecutionContext executionContext = new ExecutionContext();
//                executionContext.put("range", dateRange);
//
//                partitions.put("partition" + i, executionContext);
//
//                unique.add(dateRange);
//
//                i++;
//            }
//
//            List<HuaweiDataSyncHistory> failList = huaweiDataSyncHistoryRepository.findAllByJobNameAndProjectAndLastSyncStatusAndFailCountLessThan(
//                    JOB_NAME, PROJECT, LastSyncStatus.FAIL, 3
//            );
//
//            for (HuaweiDataSyncHistory item : failList) {
//
//                CustomDateRange dateRange = new CustomDateRange(
//                        item.getStart(), item.getEnd(), item.getEnd().getYear(), item.getEnd().getMonthValue()
//                );
//
//                if (unique.contains(dateRange)) {
//                    continue;
//                }
//
//                unique.add(dateRange);
//
//                ExecutionContext executionContext = new ExecutionContext();
//                executionContext.put("range", dateRange);
//
//                partitions.put("partition" + i, executionContext);
//
//                i++;
//            }
//
//            return partitions;
//        };
//    }
//
//    @Bean
//    public Step externalHuaweiBillingDataSlaveStep() {
//        return new StepBuilder("externalHuaweiBillingDataSlaveStep", jobRepository)
//                .tasklet((contribution, chunkContext) -> {
//
//                    StepExecution stepExecution = requireNonNull(
//                            StepSynchronizationManager.getContext()
//                    ).getStepExecution();
//
//                    CustomDateRange range = (CustomDateRange) stepExecution.getExecutionContext().get("range");
//
//                    if (range != null) {
//
//                        log.info("Processing partition {} for externalHuaweiBillingDataJob", range);
//
//                        SecretPayload secret = secretPayloadStoreService.get(SECRET_STORE_KEY);
//
//                        HuaweiAuthDetails token = huaweiAuthService.login(
//                                secret.getUsername(), secret.getPassword(), secret.getDomainName(), secret.getRegion()
//                        );
//
//                        huaweiBillingService.fetchDailyServiceCostUsage(
//                                range, token
//                        );
//
//                    }
//
//                    return RepeatStatus.FINISHED;
//                }, platformTransactionManager)
//                .listener(externalHuaweiBillingDataStepListener())
//                .build();
//    }
//
//    @Bean
//    public StepExecutionListener externalHuaweiBillingDataStepListener() {
//
//        return new StepExecutionListener() {
//
//            @Override
//            public void beforeStep(StepExecution stepExecution) {
//
//                CustomDateRange range = (CustomDateRange) stepExecution.getExecutionContext().get("range");
//                Long orgId = (Long) stepExecution.getExecutionContext().get("orgId");
//
//                if (range != null && orgId != null) {
//
//                    log.info(
//                            "Starting externalHuaweiBillingDataJob's step: {} for partition {} and organization ID {}",
//                            stepExecution.getStepName(), range, orgId
//                    );
//
//                }
//            }
//
//            @Override
//            public ExitStatus afterStep(StepExecution stepExecution) {
//
//                String partitionName = stepExecution.getStepName();
//                BatchStatus status = stepExecution.getStatus();
//
//                if (!stepExecution.getFailureExceptions().isEmpty()) {
//
//                    for (Throwable ex : stepExecution.getFailureExceptions()) {
//
//                        log.error(
//                                "ExternalHuaweiBillingDataJob exception in step {}: ", partitionName, ex
//                        );
//
//                    }
//
//                }
//
//                CustomDateRange range = (CustomDateRange) stepExecution.getExecutionContext().get("range");
//
//                if (range != null) {
//
//                    HuaweiDataSyncHistory sync = huaweiDataSyncHistoryRepository.findByJobNameAndProjectAndStartAndEnd(
//                            JOB_NAME, PROJECT, range.start(), range.end()
//                    ).orElse(new HuaweiDataSyncHistory(
//                            JOB_NAME, PROJECT, range.start(), range.end()
//                    ));
//
//                    if (status.equals(BatchStatus.COMPLETED)) {
//                        sync.setLastSyncStatus(LastSyncStatus.SUCCESS);
//                    } else {
//                        sync.setLastSyncStatus(LastSyncStatus.FAIL);
//                        sync.setFailCount(sync.getFailCount() + 1);
//                    }
//
//                    huaweiDataSyncHistoryRepository.save(sync);
//
//                }
//
//                log.info(
//                        "ExternalHuaweiBillingDataJob's Step completed: {} with status: {} for partition {}",
//                        partitionName, status, range
//                );
//
//                return stepExecution.getExitStatus();
//            }
//        };
//    }

}
