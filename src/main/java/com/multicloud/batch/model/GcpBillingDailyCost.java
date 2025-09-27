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
        name = "gcp_billing_daily_costs",
        uniqueConstraints = @UniqueConstraint(
                name = "idx_uq_const_gcp",
                columnNames = {
                        "usage_date", "billing_account_id", "project_id", "service_code", "sku_id",
                        "region", "cost_type"
                }
        ),
        indexes = {
                @Index(name = "idx_usage_date", columnList = "usage_date"),
                @Index(name = "idx_billing_account_id", columnList = "billing_account_id"),
                @Index(name = "idx_project_id", columnList = "project_id"),
                @Index(name = "idx_service_code", columnList = "service_code"),
                @Index(name = "idx_sku_id", columnList = "sku_id"),
                @Index(
                        name = "idx_project_service",
                        columnList = "usage_date, billing_account_id, project_id, service_code"
                )
        }
)
public class GcpBillingDailyCost {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "usage_date", nullable = false)
    private LocalDate usageDate;

    // Master/Billing Account ID
    @Column(name = "billing_account_id", nullable = false, length = 32)
    private String billingAccountId;

    // GCP gives per project billing
    // Usage scope
    @Column(name = "project_id", nullable = false, length = 128)
    private String projectId;

    @Column(name = "project_name", nullable = false, length = 128)
    private String projectName;

    @Column(name = "service_code", nullable = false, length = 64)
    private String serviceCode;

    @Column(name = "service_name", nullable = false, length = 128)
    private String serviceName;

    @Column(name = "sku_id", nullable = false, length = 64)
    private String skuId;

    @Column(name = "sku_description", nullable = false, length = 512)
    private String skuDescription;

    @Column(nullable = false, length = 64)
    private String region;

    @Column(nullable = false, length = 128)
    private String location;

    @Column(length = 3)
    private String currency;

    @Column(name = "cost_type", nullable = false, length = 32)
    private String costType;

    @Column(name = "usage_amount", precision = 30, scale = 8)
    private BigDecimal usageAmount;

    @Column(name = "usage_unit", length = 64)
    private String usageUnit;

    @Column(precision = 20, scale = 8)
    private BigDecimal cost;

    @Column(precision = 20, scale = 8)
    private BigDecimal credits;

}
