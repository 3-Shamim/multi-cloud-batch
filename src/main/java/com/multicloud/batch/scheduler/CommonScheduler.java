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
@RequiredArgsConstructor
@Component
public class CommonScheduler {

    @Value("${batch_job.combined_billing.enabled}")
    private boolean isCombinedBillingJobEnabled;

    private final JobLauncher jobLauncher;
    private final JobService jobService;
    private final Job combineServiceBillingDataJob;

    @Async
    @Scheduled(cron = "${batch_job.combined_billing.corn}")
    public void runCombineServiceBillingDataJob() throws Exception {

        if (!isCombinedBillingJobEnabled) {
            log.info("Skipping because the job is disabled: {}", combineServiceBillingDataJob.getName());
            return;
        }

        if (jobService.isJobTrulyRunning(combineServiceBillingDataJob.getName())) {
            log.info("Skipping because the job is already running: {}", combineServiceBillingDataJob.getName());
            return;
        }

        JobParameters jobParameters = new JobParametersBuilder()
                .addLong("time", System.currentTimeMillis())
                .toJobParameters();

        jobLauncher.run(combineServiceBillingDataJob, jobParameters);

    }

}
