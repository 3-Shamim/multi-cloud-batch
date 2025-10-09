package com.multicloud.batch.dto;

import java.math.BigDecimal;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim Molla
 * Email: shamim.molla@vivasoftltd.com
 */

public record CloudProviderCostDTO(String cloudProvider, BigDecimal cost) {
}