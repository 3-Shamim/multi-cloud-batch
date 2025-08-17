package com.multicloud.batch.dao.aws;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.multicloud.batch.util.Util;
import com.multicloud.batch.dao.aws.payload.SecretPayload;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.*;

import java.io.IOException;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim Molla
 * Email: shamim.molla@vivasoftltd.com
 */

@Slf4j
@Profile("!test")
@RequiredArgsConstructor
@Service
public class AwsSecretsManagerServiceImpl implements AwsSecretsManagerService {

    private final ObjectMapper objectMapper;
    private final SecretsManagerClient secretsManagerClient;

    @Override
    public String createSecret(String name, SecretPayload secretPayload) {

        String secret;

        try {

            secret = objectMapper.writeValueAsString(secretPayload);
            secret = Util.compressValue(secret);

        } catch (IOException e) {
            log.error("Error while converting object to secret or during compression.", e);
            throw new RuntimeException(e);
        }

        CreateSecretRequest request = CreateSecretRequest.builder()
                .name(name)
                .secretString(secret)
                .build();

        CreateSecretResponse response = secretsManagerClient.createSecret(request);

        return response.arn();
    }

    @Override
    public  SecretPayload getSecret(String secretName) {

        GetSecretValueResponse response = secretsManagerClient.getSecretValue(
                GetSecretValueRequest.builder().secretId(secretName).build()
        );

        try {

            String secret = response.secretString();
            secret = Util.decompressValue(secret);

            return objectMapper.readValue(secret, SecretPayload.class);
        } catch (IOException e) {
            log.error("Error while converting secret to object or during decompression.", e);
            throw new RuntimeException(e);
        }

    }

    @Override
    public void updateSecret(String secretName, SecretPayload secretPayload) {

        String secret;

        try {

            secret = objectMapper.writeValueAsString(secretPayload);
            secret = Util.compressValue(secret);

        } catch (IOException e) {
            log.error("Error while converting object to secret or during compression.", e);
            throw new RuntimeException(e);
        }

        UpdateSecretRequest request = UpdateSecretRequest.builder()
                .secretId(secretName)
                .secretString(secret)
                .build();

        secretsManagerClient.updateSecret(request);

    }

    @Override
    public void deleteSecret(String secretName) {

        secretsManagerClient.deleteSecret(DeleteSecretRequest.builder()
                .secretId(secretName)
                .forceDeleteWithoutRecovery(true)
                .build());

    }

}
