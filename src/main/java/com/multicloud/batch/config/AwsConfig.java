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

    // The default value is empty for all fields

    @Value(value = "${aws.access_key:}")
    private String accessKey;
    @Value(value = "${aws.secret_key:}")
    private String secretKey;
    @Value(value = "${aws.session_token:}")
    private String sessionToken;

    @Value(value = "${aws.region:}")
    private String region;

    @Value(value = "${aws.profile:}")
    private String profile;

    @Bean
    public SecretsManagerClient secretsManagerClient() {

        AwsCredentialsProvider provider;

        if (StringUtils.hasText(accessKey) && StringUtils.hasText(secretKey)) {

            if (StringUtils.hasText(sessionToken)) {
                AwsSessionCredentials credentials = AwsSessionCredentials.create(accessKey, secretKey, sessionToken);
                provider = StaticCredentialsProvider.create(credentials);
            } else {
                AwsBasicCredentials credentials = AwsBasicCredentials.create(accessKey, secretKey);
                provider = StaticCredentialsProvider.create(credentials);
            }

        } else if (StringUtils.hasText(profile)) {
            provider = ProfileCredentialsProvider.builder().profileName(profile).build();
        } else {
            provider = DefaultCredentialsProvider.builder().build();
        }

        return SecretsManagerClient.builder()
                .region(Region.of(region))
                .credentialsProvider(provider)
                .build();
    }

}
