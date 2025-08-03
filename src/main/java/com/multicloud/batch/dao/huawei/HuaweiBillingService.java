package com.multicloud.batch.dao.huawei;

import com.multicloud.batch.enums.LastSyncStatus;
import com.multicloud.batch.job.CustomDateRange;
import org.springframework.data.util.Pair;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim Molla
 * Email: shamim.molla@vivasoftltd.com
 */

public interface HuaweiBillingService {

    void fetchDailyServiceCostUsage(long organizationId, CustomDateRange customDateRange, String token);

}
