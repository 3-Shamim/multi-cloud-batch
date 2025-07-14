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
                name = "uq_aws_billing_grouped_fields",
                columnNames = {
                        "usageDate", "payerAccountId", "usageAccountId", "projectId",
                        "serviceCode", "serviceName", "skuId", "skuDescription",
                        "region", "location", "currency", "pricingType", "usageType"
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

    private String payerAccountId;
    private String usageAccountId;

    private String projectId;
    private String projectName;

    private String serviceCode;
    private String serviceName;

    private String skuId;
    private String skuDescription;

    private String region;
    private String location;

    private String currency;
    private String pricingType;
    private String usageType;

    private BigDecimal usageAmount;
    private String usageUnit;

    private BigDecimal unblendedCost;
    private BigDecimal blendedCost;
    private BigDecimal effectiveCost;

    private LocalDateTime billingPeriodStart;
    private LocalDateTime billingPeriodEnd;

}