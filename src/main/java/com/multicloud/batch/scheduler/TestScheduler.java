package com.multicloud.batch.scheduler;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
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
public class TestScheduler {

    @Value("${batch_job.sample.enabled}")
    private boolean isSampleJobEnabled;

    @Scheduled(cron = "${batch_job.sample.corn}")
    public void runTestSchedule() {

        if (!isSampleJobEnabled) {
            log.info("Skipping because the job is disabled: {}", "sample");
            return;
        }

        log.info("Test scheduler is running.");

    }

}