package com.multicloud.batch.dao.huawei;

import com.multicloud.batch.dao.huawei.payload.HuaweiAuthDetails;
import com.multicloud.batch.job.CustomDateRange;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim Molla
 * Email: shamim.molla@vivasoftltd.com
 */

public interface HuaweiBillingService {

    void fetchDailyServiceCostUsage(long organizationId, CustomDateRange customDateRange, HuaweiAuthDetails authDetails);

}
