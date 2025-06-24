package com.multicloud.batch.scheduler;

import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.stereotype.Component;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim Molla
 * Email: shamim.molla@vivasoftltd.com
 */

@Component
@RequiredArgsConstructor
public class SimpleJobScheduler {

    private final JobLauncher jobLauncher;
    private final Job sampleJob;

    //    @Scheduled(cron = "0 */1 * * * *") // every minute
    public void runJob() throws Exception {

        JobParameters jobParameters = new JobParametersBuilder()
                .addLong("time", System.currentTimeMillis())
                .toJobParameters();

        jobLauncher.run(sampleJob, jobParameters);

    }

}