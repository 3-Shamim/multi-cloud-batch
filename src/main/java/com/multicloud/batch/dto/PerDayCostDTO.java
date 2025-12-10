package com.multicloud.batch.dto;

import com.multicloud.batch.enums.CloudProvider;

import java.math.BigDecimal;
import java.time.LocalDate;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim Molla
 * Email: shamim.molla@vivasoftltd.com
 */

public record PerDayCostDTO(
        LocalDate usageDate,
        CloudProvider cloudProvider,
        BigDecimal cost,
        BigDecimal afterDiscountCost,
        BigDecimal handlingFee,
        BigDecimal supportFee
) {
}