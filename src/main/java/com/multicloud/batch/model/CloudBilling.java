package com.multicloud.batch.model;

import com.multicloud.batch.enums.CloudProvider;
import jakarta.persistence.*;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDate;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim Molla
 * Email: shamim.molla@vivasoftltd.com
 */

@Entity
@Table(name = "cloud_billings")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CloudBilling {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Positive(message = "Organization ID must be a positive number")
    @Column(nullable = false)
    private long organizationId;

    @NotNull(message = "Cloud provider must not be null")
    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 100)
    private CloudProvider cloudProvider;

    @Column(name = "account_id")
    private String accountId;

    @Column(name = "project_id")
    private String projectId;

    @Column(name = "service_name")
    private String serviceName;

    @Column(name = "sku_name", columnDefinition = "TEXT")
    private String skuName;

    @Column(name = "usage_start_date")
    private LocalDate usageStartDate;

    @Column(name = "usage_end_date")
    private LocalDate usageEndDate;

    @Column(name = "usage_amount", precision = 30, scale = 6)
    private BigDecimal usageAmount;

    @Column(name = "usage_unit")
    private String usageUnit;

    @Column(name = "cost_amount_usd", precision = 30, scale = 6)
    private BigDecimal costAmountUsd;

    @Column(name = "currency")
    private String currency;

    @Column(name = "billing_export_source")
    private String billingExportSource;

}