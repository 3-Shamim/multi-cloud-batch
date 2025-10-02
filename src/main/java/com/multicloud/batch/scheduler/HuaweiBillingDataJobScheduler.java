package com.multicloud.batch.scheduler;

import com.multicloud.batch.service.JobService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
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
@ConditionalOnExpression("${batch_job.huawei_billing_data.enabled} OR ${batch_job.external_huawei_billing_data.enabled}")
public class HuaweiBillingDataJobScheduler {

    @Value("${batch_job.huawei_billing_data.enabled}")
    private boolean huaweiBillingDataJobEnabled;
    @Value("${batch_job.external_huawei_billing_data.enabled}")
    private boolean externalHuaweiBillingDataJobEnabled;

    private final JobLauncher jobLauncher;
    private final JobService jobService;
    private final Job huaweiBillingDataJob;
    private final Job externalHuaweiBillingDataJob;

    public HuaweiBillingDataJobScheduler(JobLauncher jobLauncher,
                                         JobService jobService,
                                         @Qualifier("huaweiBillingDataJob")
                                         Job huaweiBillingDataJob,
                                         @Qualifier("externalHuaweiBillingDataJob")
                                         Job externalHuaweiBillingDataJob) {

        this.jobLauncher = jobLauncher;
        this.jobService = jobService;
        this.huaweiBillingDataJob = huaweiBillingDataJob;
        this.externalHuaweiBillingDataJob = externalHuaweiBillingDataJob;
    }

    @Async
    @Scheduled(cron = "${batch_job.huawei_billing_data.corn}")
    public void runHuaweiBillingDataJob() throws Exception {

        if (!huaweiBillingDataJobEnabled) {
            log.info("Skipping because the job huaweiBillingDataJob is disabled");
            return;
        }

        if (jobService.isJobTrulyRunning(huaweiBillingDataJob.getName())) {
            log.info(
                    "Skipping huaweiBillingDataJob because the job is already running: {}",
                    huaweiBillingDataJob.getName()
            );
            return;
        }

        JobParameters jobParameters = new JobParametersBuilder()
                .addLong("time", System.currentTimeMillis())
                .toJobParameters();

        jobLauncher.run(huaweiBillingDataJob, jobParameters);

    }


    @Async
    @Scheduled(cron = "${batch_job.external_huawei_billing_data.corn}")
    public void runExternalHuaweiBillingDataJob() throws Exception {

        if (!externalHuaweiBillingDataJobEnabled) {
            log.info("Skipping because the job externalHuaweiBillingDataJob is disabled");
            return;
        }

        if (jobService.isJobTrulyRunning(externalHuaweiBillingDataJob.getName())) {
            log.info(
                    "Skipping externalHuaweiBillingDataJob because the job is already running: {}",
                    externalHuaweiBillingDataJob.getName()
            );
            return;
        }

        JobParameters jobParameters = new JobParametersBuilder()
                .addLong("time", System.currentTimeMillis())
                .toJobParameters();

        jobLauncher.run(externalHuaweiBillingDataJob, jobParameters);

    }

}