package com.multicloud.batch.model;

import jakarta.persistence.*;
import lombok.*;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;

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
@Table(name = "aws_billing_daily_costs",
        uniqueConstraints = @UniqueConstraint(
                name = "idx_uq_const",
                columnNames = {
                        "usageDate", "payerAccountId", "usageAccountId", "projectId", "projectName",
                        "serviceCode", "serviceName", "skuId", "skuDescription", "region", "location",
                        "currency", "pricingType", "usageType"
                }
        ),
        indexes = {
                @Index(name = "idx_usage_date", columnList = "usageDate"),
                @Index(name = "idx_service_code", columnList = "serviceCode"),
                @Index(name = "idx_sku_id", columnList = "skuId"),
                @Index(name = "idx_project_id", columnList = "projectId"),
                @Index(name = "idx_payer_account_id", columnList = "payerAccountId"),
                @Index(name = "idx_usage_account_id", columnList = "usageAccountId"),
                @Index(name = "idx_service_level", columnList = "usageDate, payerAccountId, serviceCode")
        }
)
public class AwsBillingDailyCost {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private LocalDate usageDate;

    @Column(length = 12)
    private String payerAccountId;

    @Column(length = 12)
    private String usageAccountId;

    @Column(length = 64)
    private String projectId;

    @Column(length = 128)
    private String projectName;

    @Column(length = 32)
    private String serviceCode;

    @Column(length = 64)
    private String serviceName;

    @Column(length = 32)
    private String skuId;

    @Column(length = 256)
    private String skuDescription;

    @Column(length = 32)
    private String region;

    @Column(length = 64)
    private String location;

    @Column(length = 3)
    private String currency;

    @Column(length = 16)
    private String pricingType;

    @Column(length = 64)
    private String usageType;

    @Column(precision = 20, scale = 6)
    private BigDecimal usageAmount;

    @Column(length = 64)
    private String usageUnit;

    @Column(precision = 20, scale = 6)
    private BigDecimal unblendedCost;

    @Column(precision = 20, scale = 6)
    private BigDecimal blendedCost;

    @Column(precision = 20, scale = 6)
    private BigDecimal effectiveCost;

    @Column(columnDefinition = "DATETIME(0)")
    private LocalDateTime billingPeriodStart;

    @Column(columnDefinition = "DATETIME(0)")
    private LocalDateTime billingPeriodEnd;

}