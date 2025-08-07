package com.multicloud.batch.scheduler;

import com.multicloud.batch.service.JobService;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.*;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim Molla
 * Email: shamim.molla@vivasoftltd.com
 */

@Component
@RequiredArgsConstructor
public class HuaweiBillingDataJobScheduler {

    private final JobLauncher jobLauncher;
    private final JobService jobService;
    private final Job huaweiBillingDataJob;

//    @Async
//    @Scheduled(fixedDelay = 1, timeUnit = TimeUnit.DAYS)
    public void runHuaweiBillingDataJob() throws Exception {

        Thread.sleep(10000);

        if (jobService.isJobTrulyRunning(huaweiBillingDataJob.getName())) {
            return;
        }

        JobParameters jobParameters = new JobParametersBuilder()
                .addLong("time", System.currentTimeMillis())
                .addLong("orgId", 1L)
                .toJobParameters();

        jobLauncher.run(huaweiBillingDataJob, jobParameters);

    }

}