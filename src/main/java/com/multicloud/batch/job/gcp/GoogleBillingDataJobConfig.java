package com.multicloud.batch.job.gcp;

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
public class GoogleBillingDataJobConfig {

    private final JobRepository jobRepository;
    private final CloudConfigRepository cloudConfigRepository;
    private final PlatformTransactionManager platformTransactionManager;

    @Bean
    public Job googleBillingDataJob() {
        return new JobBuilder("googleBillingDataJob", jobRepository)
                .start(googleBillingDataStep())
                .build();
    }

    @Bean
    public Step googleBillingDataStep() {
        return new StepBuilder("googleBillingDataStep", jobRepository)
                .<CloudConfig, GoogleAccountConfig>chunk(10, platformTransactionManager)
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
    public ItemProcessor<CloudConfig, GoogleAccountConfig> googleBillingDataProcessor() {
        return item -> {

            log.info("Processing google billing data with creation time: {}", item);
            // In a real implementation, you would process the data here

            return new GoogleAccountConfig(
                    item.getAccessKey(),
                    item.getSecretKey(),
                    item.isConnected(),
                    item.getLastSyncTime()
            );
        };
    }

    @Bean
    public ItemWriter<GoogleAccountConfig> googleBillingDataWriter() {
        return items -> {

            for (GoogleAccountConfig item : items) {

                log.info("Writing google billing data: {}", item);
                // In a real implementation, you would write the data to a destination here

            }

        };
    }

    public record GoogleAccountConfig(String accessKey, String secretKey, boolean connected, LocalDateTime lastSyncTime) {
    }

}
