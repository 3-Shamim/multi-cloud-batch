package com.multicloud.batch.dao.aws;

import com.multicloud.batch.enums.LastSyncStatus;
import org.springframework.data.util.Pair;

import java.time.LocalDate;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim Molla
 * Email: shamim.molla@vivasoftltd.com
 */

public interface AwsBillingService {

    Pair<LastSyncStatus, String> syncDailyServiceCostUsageFromExplorer(
            long organizationId, String accessKey, String secretKey, boolean firstSync
    );

    Pair<LastSyncStatus, String> readAllAccounts(long organizationId, String accessKey, String secretKey);

    void syncDailyCostUsageFromAthena(
            long organizationId, String accessKey, String secretKey, LocalDate start, LocalDate end
    );

    boolean checkAwsExplorerConnection(String accessKey, String secretKey);

}
