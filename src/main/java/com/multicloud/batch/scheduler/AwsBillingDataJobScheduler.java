package com.multicloud.batch.scheduler;

import com.multicloud.batch.service.JobService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.configuration.JobRegistry;
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
@RequiredArgsConstructor
@ConditionalOnExpression("${batch_job.aws_billing_data.enabled} OR ${batch_job.external_aws_billing_data.enabled}")
public class AwsBillingDataJobScheduler {

    @Value("${batch_job.aws_billing_data.enabled}")
    private boolean awsBillingDataJobEnabled;
    @Value("${batch_job.external_aws_billing_data.enabled}")
    private boolean externalAwsBillingDataJobEnabled;

    private final JobRegistry jobRegistry;
    private final JobLauncher jobLauncher;
    private final JobService jobService;

    @Async
    @Scheduled(cron = "${batch_job.aws_billing_data.corn}")
    public void runAwsBillingDataJob() throws Exception {

        if (!awsBillingDataJobEnabled) {
            log.info("Skipping because the job awsBillingDataJob is disabled");
            return;
        }

        Job awsBillingDataJob = jobRegistry.getJob("awsBillingDataJob");

        if (jobService.isJobTrulyRunning(awsBillingDataJob.getName())) {
            log.info("Skipping awsBillingDataJob because the job is already running");
            return;
        }

        JobParameters jobParameters = new JobParametersBuilder()
                .addLong("time", System.currentTimeMillis())
                .toJobParameters();

        jobLauncher.run(awsBillingDataJob, jobParameters);

    }

    @Async
    @Scheduled(cron = "${batch_job.external_aws_billing_data.corn}")
    public void runExternalAwsBillingDataJob() throws Exception {

        if (!externalAwsBillingDataJobEnabled) {
            log.info("Skipping because the job externalAwsBillingDataJob is disabled");
            return;
        }

        Job externalAwsBillingDataJob = jobRegistry.getJob("externalAwsBillingDataJob");

        if (jobService.isJobTrulyRunning(externalAwsBillingDataJob.getName())) {
            log.info("Skipping externalAwsBillingDataJob because the job is already running");
            return;
        }

        JobParameters jobParameters = new JobParametersBuilder()
                .addLong("time", System.currentTimeMillis())
                .toJobParameters();

        jobLauncher.run(externalAwsBillingDataJob, jobParameters);

    }

}