package com.multicloud.batch.model;

import jakarta.persistence.*;
import lombok.*;

import java.math.BigDecimal;
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
@Builder
@ToString
@Entity
@Table(
        name = "aws_billing_daily_costs",
        uniqueConstraints = @UniqueConstraint(
                name = "idx_uq_const",
                columnNames = {
                        "organization_id", "usage_date", "payer_account_id", "usage_account_id", "service_code",
                        "service_name", "sku_id", "sku_description", "region", "location", "usage_type"
                }
        ),
        indexes = {
                @Index(name = "idx_organization_id", columnList = "organization_id"),
                @Index(name = "idx_usage_date", columnList = "usage_date"),
                @Index(name = "idx_payer_account_id", columnList = "payer_account_id"),
                @Index(name = "idx_usage_account_id", columnList = "usage_account_id"),
                @Index(name = "idx_service_code", columnList = "service_code"),
                @Index(name = "idx_sku_id", columnList = "sku_id"),
                @Index(name = "idx_service_level", columnList = "usage_date, payer_account_id, service_code")
        }
)
public class AwsBillingDailyCost {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    // Multicloud organization ID
    @Column(name = "organization_id", nullable = false)
    private long organizationId;

    @Column(name = "usage_date", nullable = false)
    private LocalDate usageDate;

    // Master/Billing Account ID
    @Column(name = "payer_account_id", nullable = false, length = 12)
    private String payerAccountId;

    // Linked/Usage Account ID
    // Usage scope
    @Column(name = "usage_account_id", nullable = false, length = 12)
    private String usageAccountId;

    @Column(name = "service_code", nullable = false, length = 32)
    private String serviceCode;

    @Column(name = "service_name", nullable = false, length = 128)
    private String serviceName;

    @Column(name = "sku_id", nullable = false, length = 32)
    private String skuId;

    @Column(name = "sku_description", nullable = false, length = 512)
    private String skuDescription;

    @Column(nullable = false, length = 32)
    private String region;

    @Column(nullable = false, length = 128)
    private String location;

    @Column(length = 3)
    private String currency;

    @Column(name = "pricing_type", length = 16)
    private String pricingType;

    @Column(name = "usage_type", nullable = false, length = 128)
    private String usageType;

    @Column(name = "usage_amount", precision = 30, scale = 8)
    private BigDecimal usageAmount;

    @Column(name = "usage_unit", length = 128)
    private String usageUnit;

    @Column(name = "unblended_cost", precision = 20, scale = 8)
    private BigDecimal unblendedCost;

    @Column(name = "blended_cost", precision = 20, scale = 8)
    private BigDecimal blendedCost;

    @Column(name = "effective_cost", precision = 20, scale = 8)
    private BigDecimal effectiveCost;

}