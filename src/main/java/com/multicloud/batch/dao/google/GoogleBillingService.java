package com.multicloud.batch.dao.google;

import com.multicloud.batch.enums.LastSyncStatus;
import org.springframework.data.util.Pair;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim Molla
 * Email: shamim.molla@vivasoftltd.com
 */

public interface GoogleBillingService {

    Pair<LastSyncStatus, String> fetchDailyServiceCostUsage(long organizationId, byte[] jsonKey, boolean firstSync);

    boolean checkGoogleBigQueryConnection(byte[] jsonKey);

}
