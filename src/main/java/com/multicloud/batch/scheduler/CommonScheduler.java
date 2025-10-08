package com.multicloud.batch.scheduler;

import com.multicloud.batch.service.JobService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim Molla
 * Email: shamim.molla@vivasoftltd.com
 */

@Slf4j
@RequiredArgsConstructor
@Component
@ConditionalOnExpression("${batch_job.merge_billing.enabled}")
public class CommonScheduler {

    private final JobLauncher jobLauncher;
    private final JobService jobService;
    private final Job mergeAwsBillingDataJob;
    private final Job mergeGcpBillingDataJob;

    @Async
    @Scheduled(cron = "${batch_job.merge_billing.corn}")
    public void runAwsMergeBillingDataJob() throws Exception {


        if (jobService.isJobTrulyRunning(mergeAwsBillingDataJob.getName())) {
            log.info("Skipping mergeAwsBillingDataJob because the job is already running");
            return;
        }

        for (long i = 1; i <= 2; i++) {

            JobParameters jobParameters = new JobParametersBuilder()
                    .addLong("time", System.currentTimeMillis())
                    .addLong("orgId", i)
                    .toJobParameters();

            jobLauncher.run(mergeAwsBillingDataJob, jobParameters);
            jobLauncher.run(mergeGcpBillingDataJob, jobParameters);

        }

    }

}
