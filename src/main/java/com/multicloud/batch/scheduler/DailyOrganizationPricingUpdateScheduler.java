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
@RequiredArgsConstructor
@Component
@ConditionalOnExpression("${batch_job.daily_org_pricing_update.enabled}")
public class DailyOrganizationPricingUpdateScheduler {

    private final Job dailyOrganizationPricingUpdateJob;

    private final JobLauncher jobLauncher;
    private final JobService jobService;

    @Async
    @Scheduled(cron = "${batch_job.daily_org_pricing_update.corn}")
    public void runMergeBillingDataJob() throws Exception {

        if (jobService.isJobTrulyRunning(dailyOrganizationPricingUpdateJob.getName())) {
            log.info("Skipping dailyOrganizationPricingUpdateJob because the job is already running");
            return;
        }

        JobParameters jobParameters = new JobParametersBuilder()
                .addLong("time", System.currentTimeMillis())
                .toJobParameters();

        jobLauncher.run(dailyOrganizationPricingUpdateJob, jobParameters);

    }

}
