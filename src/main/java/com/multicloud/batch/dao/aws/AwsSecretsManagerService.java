package com.multicloud.batch.dao.aws;

import com.multicloud.batch.dao.aws.payload.SecretPayload;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim Molla
 * Email: shamim.molla@vivasoftltd.com
 */

public interface AwsSecretsManagerService {

    String createSecret(String name, SecretPayload secretPayload);

    SecretPayload getSecret(String secretName);

    void updateSecret(String secretName, SecretPayload secretPayload);

    void deleteSecret(String secretName);

}
