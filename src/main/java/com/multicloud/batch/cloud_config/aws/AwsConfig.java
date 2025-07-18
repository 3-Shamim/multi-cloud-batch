package com.multicloud.batch.cloud_config.aws;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.costexplorer.CostExplorerClient;
import software.amazon.awssdk.services.organizations.OrganizationsClient;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim Molla
 * Email: shamim.molla@vivasoftltd.com
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

    @Bean
    public AthenaClient athenaClient() {

        return AthenaClient.builder()
                .credentialsProvider(awsDynamicCredentialsProvider())
                .region(Region.EU_WEST_1) // Use the correct region for Athena
                .build();
    }

    @Bean
    public S3Client s3Client() {

        return S3Client.builder()
                .region(Region.EU_WEST_1) // Replace it with your AWS region
                .credentialsProvider(awsDynamicCredentialsProvider())
                .build();
    }

    @Bean
    public OrganizationsClient organizationsClient() {

        return OrganizationsClient.builder()
                .credentialsProvider(awsDynamicCredentialsProvider())
                .region(Region.AWS_GLOBAL)
                .build();
    }

}
