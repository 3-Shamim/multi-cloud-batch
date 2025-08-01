package com.multicloud.batch.dao.huawei.payload;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim Molla
 * Email: shamim.molla@vivasoftltd.com
 */

public record HuaweiResourceBillingRequest(
        String cycle,
        String bill_type,
        String region,
        String include_zero_record,
        String method,
        int statistic_type,
        String query_type,
        String bill_cycle_begin,
        String bill_cycle_end,
        int offset,
        int limit
) {
}
