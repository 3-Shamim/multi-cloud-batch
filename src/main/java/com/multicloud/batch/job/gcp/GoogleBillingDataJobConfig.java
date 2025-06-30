package com.multicloud.batch.job.gcp;

import com.multicloud.batch.dao.google.GoogleBillingService;
import com.multicloud.batch.enums.CloudProvider;
import com.multicloud.batch.enums.LastSyncStatus;
import com.multicloud.batch.model.CloudConfig;
import com.multicloud.batch.repository.CloudConfigRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.data.RepositoryItemReader;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.domain.Sort;
import org.springframework.data.util.Pair;
import org.springframework.transaction.PlatformTransactionManager;

import java.util.Collections;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim Molla
 * Email: shamim.molla@vivasoftltd.com
 */

@Slf4j
@Configuration
@RequiredArgsConstructor
public class GoogleBillingDataJobConfig {

    private final JobRepository jobRepository;
    private final CloudConfigRepository cloudConfigRepository;
    private final PlatformTransactionManager platformTransactionManager;
    private final GoogleBillingService googleBillingService;

    @Bean
    public Job googleBillingDataJob() {
        return new JobBuilder("googleBillingDataJob", jobRepository)
                .start(googleBillingDataStep())
                .build();
    }

    @Bean
    public Step googleBillingDataStep() {
        return new StepBuilder("googleBillingDataStep", jobRepository)
                .<CloudConfig, CloudConfig>chunk(10, platformTransactionManager)
                .reader(googleActiveAccountReader())
                .processor(googleBillingDataProcessor())
                .writer(googleBillingDataWriter())
                .build();
    }

    @Bean
    public ItemReader<CloudConfig> googleActiveAccountReader() {

        RepositoryItemReader<CloudConfig> itemReader = new RepositoryItemReader<>();
        itemReader.setRepository(cloudConfigRepository);
        itemReader.setMethodName("findAllByCloudProviderAndDisabledFalse");
        itemReader.setArguments(List.of(CloudProvider.GCP));
        itemReader.setPageSize(100);
        itemReader.setSort(Collections.singletonMap("id", Sort.Direction.ASC));

        return itemReader;
    }

    @Bean
    public ItemProcessor<CloudConfig, CloudConfig> googleBillingDataProcessor() {
        return item -> {

            log.info("Processing google billing data with creation time: {}", item);

            return item;
        };
    }

    @Bean
    public ItemWriter<CloudConfig> googleBillingDataWriter() {
        return items -> {

            for (CloudConfig item : items) {

                Pair<LastSyncStatus, String> pair = googleBillingService.fetchDailyServiceCostUsage(
                        item.getOrganizationId(), item.getFile(), item.getLastSyncStatus()
                );

                item.setLastSyncStatus(pair.getFirst());
                item.setLastSyncMessage(pair.getSecond());

                cloudConfigRepository.save(item);


            }

        };
    }

}
