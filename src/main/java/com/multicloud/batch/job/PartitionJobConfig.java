package com.multicloud.batch.job;

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

import java.time.LocalDate;
import java.util.HashMap;
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
public class PartitionJobConfig {

    private final JobRepository jobRepository;
    private final PlatformTransactionManager platformTransactionManager;

    @Bean
    public Job partitionJob() {
        return new JobBuilder("partitionJob", jobRepository)
                .start(partitionStep())
                .build();
    }

    @Bean
    public Step partitionStep() {
        return new StepBuilder("partitionStep", jobRepository)
                .partitioner(slaveStep().getName(), partitioner())
                .step(slaveStep())
                .gridSize(5)
                .taskExecutor(taskExecutor())
                .build();
    }

    @Bean
    public SimpleAsyncTaskExecutor taskExecutor() {
        return new SimpleAsyncTaskExecutor();
    }

    @Bean
    public Partitioner partitioner() {

        return gridSize -> {

            StepExecution stepExecution = requireNonNull(StepSynchronizationManager.getContext()).getStepExecution();

            JobParameters jobParameters = stepExecution.getJobParameters();
            Long orgId = jobParameters.getLong("orgId");
            Long days = jobParameters.getLong("days");

            if (days == null || orgId == null || days < 1 || orgId < 1) {
                log.error("Invalid parameters for partition job...");
                return new HashMap<>();
            }

            Map<String, ExecutionContext> partitions = new HashMap<>();

            LocalDate now = LocalDate.now();

            int i = 1;

            while (days > 0) {

                long min = Math.min(days, 10);
                LocalDate start = now.minusDays(min);

                ExecutionContext executionContext = new ExecutionContext();
                executionContext.put("id", i);
                executionContext.put("start", start);
                executionContext.put("end", now);
                partitions.put("partition" + i, executionContext);

                now = start.minusDays(1);
                days -= min;
                i++;

            }

            return partitions;
        };
    }

    @Bean
    public Step slaveStep() {
        return new StepBuilder("slaveStep", jobRepository)
                .tasklet(
                        (contribution, chunkContext) -> {

                            StepExecution stepExecution = requireNonNull(StepSynchronizationManager.getContext()).getStepExecution();
                            Integer id = (Integer) stepExecution.getExecutionContext().get("id");
                            LocalDate start = (LocalDate) stepExecution.getExecutionContext().get("start");
                            LocalDate end = (LocalDate) stepExecution.getExecutionContext().get("end");

                            log.info("Processing partition... {} - [{} - {}]", id, start, end);

                            return RepeatStatus.FINISHED;
                        },
                        platformTransactionManager
                )
                .build();
    }

//    @Bean
//    public Step slaveStep() {
//        return new StepBuilder("slaveStep", jobRepository)
//                .<Long, Long>chunk(1, platformTransactionManager)
//                .reader(itemReader())
//                .writer(itemWriter())
//
//                // This retry only works for processor and writer
//                .faultTolerant()
//                .retry(RuntimeException.class)
//                .retryLimit(3)
//                .build();
//    }
//
//    @StepScope
//    @Bean
//    public ItemReader<Long> itemReader() {
//
//        StepExecution stepExecution = Objects.requireNonNull(StepSynchronizationManager.getContext()).getStepExecution();
//        Integer id = (Integer) stepExecution.getExecutionContext().get("id");
//
//        log.info("Reading item... {}", id);
//
//        return new ListItemReader<>(List.of(1L));
//    }
//
//    @StepScope
//    @Bean
//    public ItemWriter<Long> itemWriter() {
//        return chunk -> {
//
//            StepExecution stepExecution = Objects.requireNonNull(StepSynchronizationManager.getContext()).getStepExecution();
//            Integer id = (Integer) stepExecution.getExecutionContext().get("id");
//
//            log.info("Writing item... {}", id);
//
//            if (id != null && id == 1) {
//                throw new RuntimeException("Error writing item...");
//            }
//
//        };
//    }

}
