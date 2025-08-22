package com.multicloud.batch.scheduler;

import com.multicloud.batch.service.JobService;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim Molla
 * Email: shamim.molla@vivasoftltd.com
 */

@Component
@RequiredArgsConstructor
public class GoogleBillingDataJobScheduler {

    private final JobLauncher jobLauncher;
    private final JobService jobService;
    private final Job gcpBillingDataJob;

    @Async
    @Scheduled(cron = "${batch_job.gcp_billing_data}")
    public void runGcpBillingDataJob() throws Exception {

        if (jobService.isJobTrulyRunning(gcpBillingDataJob.getName())) {
            return;
        }

        JobParameters jobParameters = new JobParametersBuilder()
                .addLong("time", System.currentTimeMillis())
                .addLong("orgId", 1L)
                .toJobParameters();

        jobLauncher.run(gcpBillingDataJob, jobParameters);

    }

}