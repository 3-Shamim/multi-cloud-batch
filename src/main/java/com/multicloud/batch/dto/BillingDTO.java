package com.multicloud.batch.dto;

import com.multicloud.batch.enums.CloudProvider;

import java.math.BigDecimal;
import java.time.YearMonth;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim Molla
 * Email: shamim.molla@vivasoftltd.com
 */

public record BillingDTO(
        YearMonth month,
        long productId,
        long organizationId,
        CloudProvider provider,
        BigDecimal cost
) {
}
