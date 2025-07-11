package com.multicloud.batch.dao.aws;

import com.multicloud.batch.enums.LastSyncStatus;
import org.springframework.data.util.Pair;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim Molla
 * Email: shamim.molla@vivasoftltd.com
 */

public interface AwsBillingService {

    Pair<LastSyncStatus, String> syncDailyServiceCostUsageFromExplorer(
            long organizationId, String accessKey, String secretKey, boolean firstSync
    );

    Pair<LastSyncStatus, String> syncDailyCostUsageFromAthena(
            long organizationId, String accessKey, String secretKey, boolean firstSync
    );

    boolean checkAwsExplorerConnection(String accessKey, String secretKey);

}
