package com.multicloud.batch.dao.google;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim Molla
 * Email: shamim.molla@vivasoftltd.com
 */

public interface GoogleBillingService {

    void fetchDailyServiceCostUsage(byte[] jsonKey, long organizationId);

    boolean checkGoogleBigQueryConnection(byte[] jsonKey);

}
