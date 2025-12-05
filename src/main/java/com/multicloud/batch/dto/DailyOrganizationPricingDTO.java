package com.multicloud.batch.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.multicloud.batch.enums.CloudProvider;
import lombok.*;

import java.time.LocalDate;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim Molla
 * Email: shamim.molla@vivasoftltd.com
 */

@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
@ToString
@Builder
@JsonIgnoreProperties(ignoreUnknown = true)
public class DailyOrganizationPricingDTO {

    private long id;
    private LocalDate pricingDate;
    private long organizationId;
    private String organizationName;
    private CloudProvider cloudProvider;
    private double discount;
    private double handlingFee;
    private double supportFee;

}
