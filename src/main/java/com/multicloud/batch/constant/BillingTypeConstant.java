package com.multicloud.batch.constant;

import java.util.Set;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim Molla
 * Email: shamim.molla@vivasoftltd.com
 */

public class BillingTypeConstant {

    public static final Set<String> REGULAR = Set.of(
            // AWS
            "Usage", "DiscountedUsage", "SavingsPlanCoveredUsage",
            // GCP
            "regular",
            // Huawei
            "1", "2", "3", "5", "8", "17"
    );

    public static final Set<String> FEE = Set.of(
            // AWS
            "Fee", "RIFee", "SavingsPlanRecurringFee",
            // Huawei
            "14", "20", "24"
    );

    public static final Set<String> DISCOUNT = Set.of(
            // AWS
            "BundledDiscount", "PrivateRateDiscount", "EdpDiscount", "SavingsPlanNegation", "Credit", "Refund",
            // GCP
            "adjustment", "rounding_error",
            // Huawei
            "4", "9", "16", "101", "102"
    );

    public static final Set<String> TAX = Set.of(
            // AWS
            "Tax",
            // GCP
            "tax",
            // Huawei
            "15", "100"
    );

}
