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
@Component
@RequiredArgsConstructor
@ConditionalOnExpression("${batch_job.gcp_billing_data.enabled}")
public class GoogleBillingDataJobScheduler {

    private final JobLauncher jobLauncher;
    private final JobService jobService;
    private final Job gcpBillingDataJob;

    @Async
    @Scheduled(cron = "${batch_job.gcp_billing_data.corn}")
    public void runGcpBillingDataJob() throws Exception {


        if (jobService.isJobTrulyRunning(gcpBillingDataJob.getName())) {
            log.info("Skipping because the job is already running: {}", gcpBillingDataJob.getName());
            return;
        }

        JobParameters jobParameters = new JobParametersBuilder()
                .addLong("time", System.currentTimeMillis())
                .toJobParameters();

        jobLauncher.run(gcpBillingDataJob, jobParameters);

    }

}