package com.multicloud.batch.scheduler;

import com.multicloud.batch.service.JobService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.*;
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
public class HuaweiBillingDataJobScheduler {

    @Value("${batch_job.huawei_billing_data.enabled}")
    private boolean isHuaweiBillingDataJobEnabled;

    private final JobLauncher jobLauncher;
    private final JobService jobService;
    private final Job huaweiBillingDataJob;

    @Async
    @Scheduled(cron = "${batch_job.huawei_billing_data.corn}")
    public void runHuaweiBillingDataJob() throws Exception {

        if (!isHuaweiBillingDataJobEnabled) {
            log.info("Skipping because the job is disabled: {}", huaweiBillingDataJob.getName());
            return;
        }

        if (jobService.isJobTrulyRunning(huaweiBillingDataJob.getName())) {
            log.info("Skipping because the job is already running: {}", huaweiBillingDataJob.getName());
            return;
        }

        JobParameters jobParameters = new JobParametersBuilder()
                .addLong("time", System.currentTimeMillis())
                .addLong("orgId", 1L)
                .toJobParameters();

        jobLauncher.run(huaweiBillingDataJob, jobParameters);

    }

}