package com.multicloud.batch.dto;

import java.math.BigDecimal;
import java.time.LocalDate;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim Molla
 * Email: shamim.molla@vivasoftltd.com
 */

public record PerDayCostDTO(LocalDate usageDate, BigDecimal cost, BigDecimal handlingFee, BigDecimal supportFee) {
}