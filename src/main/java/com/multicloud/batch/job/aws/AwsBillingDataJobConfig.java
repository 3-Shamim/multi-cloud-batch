package com.multicloud.batch.job.aws;

import com.multicloud.batch.dao.aws.AwsBillingService;
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
public class AwsBillingDataJobConfig {

    private final JobRepository jobRepository;
    private final CloudConfigRepository cloudConfigRepository;
    private final PlatformTransactionManager platformTransactionManager;
    private final AwsBillingService awsBillingService;

    @Bean
    public Job awsBillingDataJob() {
        return new JobBuilder("awsBillingDataJob", jobRepository)
                .start(awsBillingDataStep())
                .build();
    }

    @Bean
    public Step awsBillingDataStep() {
        return new StepBuilder("awsBillingDataStep", jobRepository)
                .<CloudConfig, CloudConfig>chunk(10, platformTransactionManager)
                .reader(awsActiveAccountReader())
                .processor(awsBillingDataProcessor())
                .writer(awsBillingDataWriter())
                .build();
    }

    @Bean
    public ItemReader<CloudConfig> awsActiveAccountReader() {

        RepositoryItemReader<CloudConfig> itemReader = new RepositoryItemReader<>();
        itemReader.setRepository(cloudConfigRepository);
        itemReader.setMethodName("findAllByCloudProviderAndDisabledFalse");
        itemReader.setArguments(List.of(CloudProvider.AWS));
        itemReader.setPageSize(100);
        itemReader.setSort(Collections.singletonMap("id", Sort.Direction.ASC));

        return itemReader;
    }

    @Bean
    public ItemProcessor<CloudConfig, CloudConfig> awsBillingDataProcessor() {
        return item -> {

            log.info("Processing AWS billing data with creation time: {}", item);

            return item;
        };
    }

    @Bean
    public ItemWriter<CloudConfig> awsBillingDataWriter() {
        return items -> {

            for (CloudConfig item : items) {

                log.info("Writing AWS billing data: {}", item);

                Pair<LastSyncStatus, String> pair = awsBillingService.fetchDailyServiceCostUsage(
                        item.getOrganizationId(), item.getAccessKey(), item.getSecretKey(), item.getLastSyncStatus()
                );

                item.setLastSyncStatus(pair.getFirst());
                item.setLastSyncMessage(pair.getSecond());

                cloudConfigRepository.save(item);

            }

        };
    }

}
