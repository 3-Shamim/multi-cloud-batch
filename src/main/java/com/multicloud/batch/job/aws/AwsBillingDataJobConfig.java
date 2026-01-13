package com.multicloud.batch.job.aws;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;

import java.time.LocalDate;
import java.util.Set;

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

    public static final String JOB_NAME = "awsBillingDataJob";

    private final JobRepository jobRepository;
    private final PlatformTransactionManager platformTransactionManager;

    private final JdbcTemplate jdbcTemplate;

    private final Step awsBillingDataOneMasterStep;
    private final Step awsBillingDataTwoMasterStep;

    @Bean
    public Job awsBillingDataJob() {
        return new JobBuilder(JOB_NAME, jobRepository)
                .start(lastMonthAwsDataCleanupStep())
                .next(awsBillingDataOneMasterStep)
                .next(awsBillingDataTwoMasterStep)
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

                        String query = "delete from aws_billing_daily_costs where usage_date >= ? and billing_month >= ?";

                        LocalDate firstDayOfLastMonth = now.minusMonths(1).withDayOfMonth(1);

                        jdbcTemplate.update(query, firstDayOfLastMonth, firstDayOfLastMonth);

                    }

                    return RepeatStatus.FINISHED;
                }, platformTransactionManager).build();
    }

}
