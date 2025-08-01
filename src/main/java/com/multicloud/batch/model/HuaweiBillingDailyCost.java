package com.multicloud.batch.model;

import jakarta.persistence.*;
import lombok.*;

import java.math.BigDecimal;
import java.time.LocalDate;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim
 * Date: 8/1/25
 * Email: mdshamim723@gmail.com
 */

@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
@ToString
@EqualsAndHashCode(of = {
        "organizationId", "billDate", "payerAccountId", "customerId", "cloudServiceType", "skuCode", "resourceTypeCode",
        "region", "chargeMode"
})
@Entity
@Table(
        name = "huawei_billing_daily_costs",
        uniqueConstraints = @UniqueConstraint(
                name = "idx_uq_const_huawei",
                columnNames = {
                        "organizationId", "billDate", "payerAccountId", "customerId", "cloudServiceType", "skuCode",
                        "resourceTypeCode", "region", "chargeMode"
                }
        ),
        indexes = {
                @Index(name = "idx_organization_id", columnList = "organizationId"),
                @Index(name = "idx_bill_date", columnList = "billDate"),
                @Index(name = "idx_payer_account_id", columnList = "payerAccountId"),
                @Index(name = "idx_customer_id", columnList = "customerId"),
                @Index(name = "idx_cloud_service_type", columnList = "cloudServiceType"),
                @Index(name = "idx_sku_code", columnList = "skuCode"),
                @Index(name = "idx_project_service", columnList = "billDate, customerId, cloudServiceType")
        }
)
public class HuaweiBillingDailyCost {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private long id;

    // Multicloud organization ID
    @Column(nullable = false)
    private long organizationId;

    private LocalDate billDate;

    // Master/Billing Account ID
    @Column(length = 64, nullable = false)
    private String payerAccountId;

    // Usage Account ID
    // Usage scope
    @Column(length = 64, nullable = false)
    private String customerId;

    // Optional
    private String enterpriseProjectId;
    private String enterpriseProjectName;

    @Column(length = 256, nullable = false)
    private String cloudServiceType;
    @Column(length = 200)
    private String cloudServiceTypeName;

    @Column(length = 64, nullable = false)
    private String skuCode;
    private String productSpecDesc;

    @Column(length = 64, nullable = false)
    private String resourceTypeCode;
    @Column(length = 200)
    private String resourceTypeName;

    // Optional
    private String resourceName;

    @Column(length = 64, nullable = false)
    private String region;
    @Column(length = 64)
    private String regionName;

    // 1: Yearly/monthly
    // 3: Pay-per-use
    // 10: Reserved instances
    private Integer chargeMode;

    private BigDecimal usageAmount;

    private BigDecimal consumeAmount;
    private BigDecimal officialAmount;
    private BigDecimal discountAmount;
    private BigDecimal couponAmount;

}
