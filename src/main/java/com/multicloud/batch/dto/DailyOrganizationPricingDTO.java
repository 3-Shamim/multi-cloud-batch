package com.multicloud.batch.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.multicloud.batch.enums.CloudProvider;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import lombok.*;
import org.hibernate.validator.constraints.Range;

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

    @NotNull(message = "Date must not be null")
    private LocalDate date;

    @Positive(message = "Organization ID must be a positive number")
    private long organizationId;
    private String organizationName;

    @NotNull(message = "Cloud provider must not be null")
    private CloudProvider cloudProvider;

    @NotNull(message = "Discount must not be null")
    @Range(min = 0, max = 100, message = "Discount must be between {min} to {max}")
    private double discount;

}
