package com.multicloud.batch.cloud_config.aws;

import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim Molla
 * Email: shamim.molla@vivasoftltd.com
 */

public class AwsDynamicCredentialsProvider implements AwsCredentialsProvider {

    private static final ThreadLocal<AwsCredentials> AWS_CREDENTIALS_HOLDER = new ThreadLocal<>();

    public static void setAwsCredentials(AwsCredentials awsCredentials) {
        AWS_CREDENTIALS_HOLDER.set(awsCredentials);
    }

    public static void clear() {
        AWS_CREDENTIALS_HOLDER.remove();
    }

    @Override
    public AwsCredentials resolveCredentials() {

        AwsCredentials credentials = AWS_CREDENTIALS_HOLDER.get();

        if (credentials == null) {
            throw new IllegalStateException("No credentials set for this thread");
        }

        return credentials;
    }

}
