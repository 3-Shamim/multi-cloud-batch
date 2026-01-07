package com.multicloud.batch.dao.huawei.payload;

import java.time.LocalDate;
import java.util.Comparator;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim Molla
 * Email: shamim.molla@vivasoftltd.com
 */

public record HuaweiBillingGroup(
        LocalDate billDate,
        LocalDate billingMonth,
        String payerAccountId,
        String customerId,
        String enterpriseProjectId,
        String cloudServiceType,
        String skuCode,
        String resourceTypeCode,
        String region,
        Integer chargeMode,
        Integer billType
) implements Comparable<HuaweiBillingGroup> {

    private static final Comparator<HuaweiBillingGroup> COMPARATOR =
            Comparator.comparing(HuaweiBillingGroup::billDate, Comparator.nullsFirst(LocalDate::compareTo))
                    .thenComparing(HuaweiBillingGroup::billingMonth, Comparator.nullsFirst(LocalDate::compareTo))
                    .thenComparing(HuaweiBillingGroup::payerAccountId, Comparator.nullsFirst(String::compareTo))
                    .thenComparing(HuaweiBillingGroup::customerId, Comparator.nullsFirst(String::compareTo))
                    .thenComparing(HuaweiBillingGroup::enterpriseProjectId, Comparator.nullsFirst(String::compareTo))
                    .thenComparing(HuaweiBillingGroup::cloudServiceType, Comparator.nullsFirst(String::compareTo))
                    .thenComparing(HuaweiBillingGroup::skuCode, Comparator.nullsFirst(String::compareTo))
                    .thenComparing(HuaweiBillingGroup::resourceTypeCode, Comparator.nullsFirst(String::compareTo))
                    .thenComparing(HuaweiBillingGroup::region, Comparator.nullsFirst(String::compareTo))
                    .thenComparing(HuaweiBillingGroup::chargeMode, Comparator.nullsFirst(Integer::compareTo))
                    .thenComparing(HuaweiBillingGroup::billType, Comparator.nullsFirst(Integer::compareTo));

    @Override
    public int compareTo(HuaweiBillingGroup other) {
        return COMPARATOR.compare(this, other);
    }

}
