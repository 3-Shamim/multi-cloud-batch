package com.multicloud.batch.dto;

import com.multicloud.batch.enums.CloudProvider;

import java.time.LocalDate;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim Molla
 * Email: shamim.molla@vivasoftltd.com
 */

public record OrganizationPricingDTO(long organizationId, CloudProvider provider, double discount, double handlingFee,
                                     double supportFee, LocalDate startDate) {
}
