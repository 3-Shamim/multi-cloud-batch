package com.multicloud.batch.scheduler;

import com.multicloud.batch.service.JobService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim Molla
 * Email: shamim.molla@vivasoftltd.com
 */

@Slf4j
@Component
@RequiredArgsConstructor
public class AwsBillingDataJobScheduler {

    @Value("${batch_job.aws_billing_data.enabled}")
    private boolean isAwsBillingDataJobEnabled;

    private final JobLauncher jobLauncher;
    private final JobService jobService;
    private final Job awsBillingDataJob;

    @Async
    @Scheduled(cron = "${batch_job.aws_billing_data.corn}")
    public void runAwsBillingDataJob() throws Exception {

        if (!isAwsBillingDataJobEnabled) {
            log.info("Skipping because the job is disabled: {}", awsBillingDataJob.getName());
            return;
        }

        if (jobService.isJobTrulyRunning(awsBillingDataJob.getName())) {
            log.info("Skipping because the job is already running: {}", awsBillingDataJob.getName());
            return;
        }

        JobParameters jobParameters = new JobParametersBuilder()
                .addLong("time", System.currentTimeMillis())
                .addLong("orgId", 1L)
                .toJobParameters();

        jobLauncher.run(awsBillingDataJob, jobParameters);

    }

}