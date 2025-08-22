package com.multicloud.batch.scheduler;

import com.multicloud.batch.service.JobService;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim Molla
 * Email: shamim.molla@vivasoftltd.com
 */

@Component
@RequiredArgsConstructor
public class AwsBillingDataJobScheduler {

    private final JobLauncher jobLauncher;
    private final JobService jobService;
    private final Job awsBillingDataJob;

    @Async
    @Scheduled(cron = "${batch_job.aws_billing_data}")
    public void runAwsBillingDataJob() throws Exception {

        Thread.sleep(5000);

        if (jobService.isJobTrulyRunning(awsBillingDataJob.getName())) {
            return;
        }

        JobParameters jobParameters = new JobParametersBuilder()
                .addLong("time", System.currentTimeMillis())
                .addLong("orgId", 1L)
                .toJobParameters();

        jobLauncher.run(awsBillingDataJob, jobParameters);

    }

}