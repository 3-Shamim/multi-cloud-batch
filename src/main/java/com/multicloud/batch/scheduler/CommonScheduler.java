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

import java.util.concurrent.TimeUnit;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim Molla
 * Email: shamim.molla@vivasoftltd.com
 */

@RequiredArgsConstructor
@Component
public class CommonScheduler {

    private final JobLauncher jobLauncher;
    private final JobService jobService;
    private final Job combineServiceBillingDataJob;

    @Async
    @Scheduled(fixedDelay = 1, timeUnit = TimeUnit.DAYS)
    public void runCombineServiceBillingDataJob() throws Exception {

        if (jobService.isJobTrulyRunning(combineServiceBillingDataJob.getName())) {
            return;
        }

        Thread.sleep(10000);

        JobParameters jobParameters = new JobParametersBuilder()
                .addLong("time", System.currentTimeMillis())
                .toJobParameters();

        jobLauncher.run(combineServiceBillingDataJob, jobParameters);

    }

}
