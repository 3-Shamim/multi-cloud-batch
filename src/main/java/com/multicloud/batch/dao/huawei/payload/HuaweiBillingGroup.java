package com.multicloud.batch.dao.huawei.payload;

import java.time.LocalDate;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim Molla
 * Email: shamim.molla@vivasoftltd.com
 */

public record HuaweiBillingGroup(
        LocalDate billDate,
        String payerAccountId,
        String customerId,
        String cloudServiceType,
        String skuCode,
        String resourceTypeCode,
        String region,
        Integer chargeMode
) {
}
