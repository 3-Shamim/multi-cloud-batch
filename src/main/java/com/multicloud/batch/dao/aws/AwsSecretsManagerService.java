package com.multicloud.batch.dao.aws;

import com.multicloud.batch.dao.aws.payload.SecretPayload;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim Molla
 * Email: shamim.molla@vivasoftltd.com
 */

public interface AwsSecretsManagerService {

    String createSecret(String name, SecretPayload secretPayload, boolean compress);

    SecretPayload getSecret(String secretName, boolean decompress);

    void updateSecret(String secretName, SecretPayload secretPayload, boolean compress);

    void deleteSecret(String secretName);

}
