package com.multicloud.batch.job.aws;

import com.multicloud.batch.enums.CloudProvider;
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
import org.springframework.transaction.PlatformTransactionManager;

import java.time.LocalDateTime;
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

    @Bean
    public Job awsBillingDataJob() {
        return new JobBuilder("awsBillingDataJob", jobRepository)
                .start(awsBillingDataStep())
                .build();
    }

    @Bean
    public Step awsBillingDataStep() {
        return new StepBuilder("awsBillingDataStep", jobRepository)
                .<CloudConfig, AwsAccountConfig>chunk(10, platformTransactionManager)
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
    public ItemProcessor<CloudConfig, AwsAccountConfig> awsBillingDataProcessor() {
        return item -> {

            log.info("Processing AWS billing data with creation time: {}", item);
            // In a real implementation, you would process the data here

            return new AwsAccountConfig(
                    item.getAccessKey(),
                    item.getSecretKey(),
                    item.isConnected(),
                    item.getLastSyncTime()
            );
        };
    }

    @Bean
    public ItemWriter<AwsAccountConfig> awsBillingDataWriter() {
        return items -> {

            for (AwsAccountConfig item : items) {

                log.info("Writing AWS billing data: {}", item);
                // In a real implementation, you would write the data to a destination here

            }

        };
    }

    public record AwsAccountConfig(String accessKey, String secretKey, boolean connected, LocalDateTime lastSyncTime) {
    }

}
