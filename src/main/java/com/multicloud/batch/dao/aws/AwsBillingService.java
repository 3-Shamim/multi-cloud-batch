package com.multicloud.batch.dao.aws;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim Molla
 * Email: shamim.molla@vivasoftltd.com
 */

public interface AwsBillingService {

    void fetchDailyServiceCostUsage(String accessKey, String secretKey, long organizationId);

    boolean checkAwsExplorerConnection(String accessKey, String secretKey);

}
