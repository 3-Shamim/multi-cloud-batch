package com.multicloud.batch.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.StringUtils;
import software.amazon.awssdk.auth.credentials.*;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim Molla
 * Email: shamim.molla@vivasoftltd.com
 */

@Configuration
public class AwsConfig {

    @Value("${aws.access_key:null}")
    private String accessKey;
    @Value("${aws.secret_key:null}")
    private String secretKey;

    @Value("${aws.region:null}")
    private String region;

    @Value("${aws.profile:default}")
    private String profile;

    @Bean
    public SecretsManagerClient secretsManagerClient() {

        AwsCredentialsProvider provider;

        if (StringUtils.hasText(accessKey) && StringUtils.hasText(secretKey)) {
            AwsBasicCredentials credentials = AwsBasicCredentials.create(accessKey, secretKey);
            provider = StaticCredentialsProvider.create(credentials);
        } else if (StringUtils.hasText(profile)) {
            provider = ProfileCredentialsProvider.create(profile);
        } else {
            provider = DefaultCredentialsProvider.create();
        }

        return SecretsManagerClient.builder()
                .region(Region.of(region))
                .credentialsProvider(provider)
                .build();
    }

}
