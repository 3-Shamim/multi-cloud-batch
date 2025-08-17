package com.multicloud.batch.cloud_config.aws;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim Molla
 * Email: shamim.molla@vivasoftltd.com
 */

@Configuration
public class AwsConfig {

    @Bean
    public SecretsManagerClient secretsManagerClient() {

        return SecretsManagerClient.builder()
                .region(Region.of("ap-south-1"))
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build();
    }

}
