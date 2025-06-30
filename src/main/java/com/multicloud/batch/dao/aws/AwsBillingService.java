package com.multicloud.batch.dao.aws;

import com.multicloud.batch.enums.LastSyncStatus;
import org.springframework.data.util.Pair;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim Molla
 * Email: shamim.molla@vivasoftltd.com
 */

public interface AwsBillingService {

    Pair<LastSyncStatus, String> fetchDailyServiceCostUsage(
            long organizationId, String accessKey, String secretKey, LastSyncStatus lastSyncStatus
    );

    boolean checkAwsExplorerConnection(String accessKey, String secretKey);

}
