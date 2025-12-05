package com.multicloud.batch.scheduler;

import com.multicloud.batch.service.JobService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim Molla
 * Email: shamim.molla@vivasoftltd.com
 */

@Slf4j
@RequiredArgsConstructor
@Component
@ConditionalOnExpression("${batch_job.aws_customer_cost.enabled}")
public class CalculateAwsCustomerCostScheduler {

    private final Job calculateAwsCustomerCostJob;

    private final JobLauncher jobLauncher;
    private final JobService jobService;

    @Async
    @Scheduled(cron = "${batch_job.aws_customer_cost.corn}")
    public void runMonthlyInvoiceJob() throws Exception {

        if (jobService.isJobTrulyRunning(calculateAwsCustomerCostJob.getName())) {
            log.info("Skipping calculateAwsCustomerCostJob because the job is already running");
            return;
        }

        JobParameters jobParameters = new JobParametersBuilder()
                .addLong("time", System.currentTimeMillis())
                .toJobParameters();

        jobLauncher.run(calculateAwsCustomerCostJob, jobParameters);

    }

}
