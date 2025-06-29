package com.multicloud.batch.cloud_config.aws;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.costexplorer.CostExplorerClient;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim
 * Date: 5/28/25
 * Email: mdshamim723@gmail.com
 */

@Configuration
public class AwsConfig {

    @Bean
    public AwsDynamicCredentialsProvider awsDynamicCredentialsProvider() {
        return new AwsDynamicCredentialsProvider();
    }

    @Bean
    public CostExplorerClient costExplorerClient() {

        return CostExplorerClient.builder()
                .region(Region.AWS_GLOBAL)
                .credentialsProvider(awsDynamicCredentialsProvider())
                .build();
    }

}
