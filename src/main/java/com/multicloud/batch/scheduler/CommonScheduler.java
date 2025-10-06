package com.multicloud.batch.scheduler;

import com.multicloud.batch.service.JobService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
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
@ConditionalOnProperty(name = "batch_job.merge_billing.enabled", havingValue = "true")
public class CommonScheduler {

    private final JobLauncher jobLauncher;
    private final JobService jobService;
    private final Job combineServiceBillingDataJob;

    @Async
//    @Scheduled(cron = "${batch_job.merge_billing.corn}")
    @Scheduled(fixedDelay = 1, timeUnit = TimeUnit.DAYS)
    public void runCombineServiceBillingDataJob() throws Exception {

        if (jobService.isJobTrulyRunning(combineServiceBillingDataJob.getName())) {
            log.info("Skipping because the job is already running: {}", combineServiceBillingDataJob.getName());
            return;
        }

        for (long i = 1; i <= 2; i++) {

            JobParameters jobParameters = new JobParametersBuilder()
                    .addLong("time", System.currentTimeMillis())
                    .addLong("orgId", i)
                    .toJobParameters();

            jobLauncher.run(combineServiceBillingDataJob, jobParameters);

        }


    }

}
