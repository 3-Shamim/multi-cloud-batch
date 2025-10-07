package com.multicloud.batch.job;

import com.multicloud.batch.service.ServiceTypeService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim Molla
 * Email: shamim.molla@vivasoftltd.com
 */

@Slf4j
@Configuration
@RequiredArgsConstructor
public class CacheServiceTypeStepConfig {

    private final PlatformTransactionManager platformTransactionManager;
    private final JobRepository jobRepository;

    private final ServiceTypeService serviceTypeService;

    @Bean
    public Step cacheServiceTypeStep() {
        return new StepBuilder("cacheServiceTypeStep", jobRepository)
                .tasklet((contribution, chunkContext) -> {

                    log.info("Fetching and storing service type to map");
                    serviceTypeService.fetchAndStoreServiceTypeToMap();

                    return RepeatStatus.FINISHED;
                }, platformTransactionManager)
                .build();
    }

}
