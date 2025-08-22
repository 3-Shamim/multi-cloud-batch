package com.multicloud.batch.scheduler;

import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
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
public class SampleJobScheduler {

    private final JobLauncher jobLauncher;
    private final Job sampleJob;
    private final Job partitionJob;

//    @Scheduled(cron = "${batch_job.sample}") // every minute
    public void runSampleJob() throws Exception {

        JobParameters jobParameters = new JobParametersBuilder()
                .addLong("time", System.currentTimeMillis())
                .toJobParameters();

        jobLauncher.run(sampleJob, jobParameters);

    }

//    @Scheduled(fixedDelay = 1, timeUnit = TimeUnit.DAYS) // every minute
    public void runPartitionJob() throws Exception {

        JobParameters jobParameters = new JobParametersBuilder()
                .addLong("time", System.currentTimeMillis())
                .addLong("orgId", 1L)
                .addLong("days", 365L)
                .toJobParameters();

        jobLauncher.run(partitionJob, jobParameters);

    }

}