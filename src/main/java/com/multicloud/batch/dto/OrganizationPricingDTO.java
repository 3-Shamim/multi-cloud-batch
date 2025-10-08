package com.multicloud.batch.dto;

import java.time.LocalDate;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim Molla
 * Email: shamim.molla@vivasoftltd.com
 */

public record OrganizationPricingDTO(long organizationId, LocalDate startDate, double discount, double serviceFee) {
}
