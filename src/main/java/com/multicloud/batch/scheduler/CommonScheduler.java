package com.multicloud.batch.scheduler;

import com.multicloud.batch.dto.OrganizationDTO;
import com.multicloud.batch.service.OrganizationService;
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

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim Molla
 * Email: shamim.molla@vivasoftltd.com
 */

@Slf4j
@RequiredArgsConstructor
@Component
@ConditionalOnExpression("${batch_job.merge_billing.enabled}")
public class CommonScheduler {

    private final JobLauncher jobLauncher;

    private final OrganizationService organizationService;

    private final Job mergeAwsBillingDataJob;
    private final Job mergeGcpBillingDataJob;
    private final Job mergeHuaweiBillingDataJob;

    @Async
//    @Scheduled(cron = "${batch_job.merge_billing.corn}")
    @Scheduled(fixedDelay = 1, timeUnit = TimeUnit.DAYS)
    public void runAwsMergeBillingDataJob() throws Exception {

        List<OrganizationDTO> organizations = organizationService.findOrganizations();

        for (OrganizationDTO organization : organizations) {

            JobParameters jobParameters = new JobParametersBuilder()
                    .addLong("time", System.currentTimeMillis())
                    .addLong("orgId", organization.id())
                    .toJobParameters();

//            if (!organization.skipAwsJob()) {
//                jobLauncher.run(mergeAwsBillingDataJob, jobParameters);
//            }
//
//            jobLauncher.run(mergeGcpBillingDataJob, jobParameters);
            jobLauncher.run(mergeHuaweiBillingDataJob, jobParameters);

        }

    }

}
