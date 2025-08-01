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
@Table(
        name = "gcp_billing_daily_costs",
        uniqueConstraints = @UniqueConstraint(
                name = "idx_uq_const_gcp",
                columnNames = {
                        "organizationId", "usageDate", "billingAccountId", "projectId", "projectName",
                        "serviceCode", "serviceName", "skuId", "skuDescription", "region", "location",
                        "currency", "costType", "usageUnit"
                }
        ),
        indexes = {
                @Index(name = "idx_organization_id", columnList = "organizationId"),
                @Index(name = "idx_usage_date", columnList = "usageDate"),
                @Index(name = "idx_billing_account_id", columnList = "billingAccountId"),
                @Index(name = "idx_project_id", columnList = "projectId"),
                @Index(name = "idx_service_code", columnList = "serviceCode"),
                @Index(name = "idx_sku_id", columnList = "skuId"),
                @Index(name = "idx_project_service", columnList = "usageDate, billingAccountId, serviceCode")
        }
)
public class GcpBillingDailyCost {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    // Multicloud organization ID
    @Column(nullable = false)
    private long organizationId;

    private LocalDate usageDate;

    // Master/Billing Account ID
    @Column(length = 32)
    private String billingAccountId;

    // GCP gives per project billing
    // Usage scope
    @Column(length = 128)
    private String projectId;

    @Column(length = 128)
    private String projectName;

    @Column(length = 64)
    private String serviceCode;

    @Column(length = 128)
    private String serviceName;

    @Column(length = 64)
    private String skuId;

    @Column(length = 512)
    private String skuDescription;

    @Column(length = 64)
    private String region;

    @Column(length = 128)
    private String location;

    @Column(length = 3)
    private String currency;

    @Column(length = 32)
    private String costType;

    @Column(precision = 30, scale = 6)
    private BigDecimal usageAmount;

    @Column(length = 64)
    private String usageUnit;

    @Column(precision = 20, scale = 6)
    private BigDecimal cost;

    @Column(columnDefinition = "DATETIME(0)")
    private LocalDateTime billingPeriodStart;

    @Column(columnDefinition = "DATETIME(0)")
    private LocalDateTime billingPeriodEnd;

}
