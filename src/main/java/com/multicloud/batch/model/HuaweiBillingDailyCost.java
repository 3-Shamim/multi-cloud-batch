package com.multicloud.batch.model;

import com.multicloud.batch.dao.huawei.payload.HuaweiResourceBillingResponse;
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
@Builder
@ToString
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
    @Column(length = 64)
    private String payerAccountId;

    // Usage Account ID
    // Usage scope
    @Column(length = 64)
    private String customerId;

    // Optional
    private String enterpriseProjectId;
    private String enterpriseProjectName;

    @Column(length = 256)
    private String cloudServiceType;
    @Column(length = 200)
    private String cloudServiceTypeName;

    @Column(length = 64)
    private String skuCode;
    private String productSpecDesc;

    @Column(length = 64)
    private String resourceTypeCode;
    @Column(length = 200)
    private String resourceTypeName;

    // Optional
    private String resourceName;

    @Column(length = 64)
    private String region;
    @Column(length = 64)
    private String regionName;

    // 1: Yearly/monthly
    // 3: Pay-per-use
    // 10: Reserved instances
    private Integer chargeMode;

    @Column(precision = 30, scale = 8)
    private BigDecimal usageAmount;

    @Column(precision = 20, scale = 8)
    private BigDecimal consumeAmount;
    @Column(precision = 20, scale = 8)
    private BigDecimal officialAmount;
    @Column(precision = 20, scale = 8)
    private BigDecimal discountAmount;
    @Column(precision = 20, scale = 8)
    private BigDecimal couponAmount;

    public static HuaweiBillingDailyCost from(HuaweiResourceBillingResponse.MonthlyRecord record, long organizationId) {

        return HuaweiBillingDailyCost.builder()
                .organizationId(organizationId)
                .billDate(LocalDate.parse(record.bill_date()))
                .payerAccountId(record.payer_account_id())
                .customerId(record.customer_id())
                .enterpriseProjectId(record.enterprise_project_id())
                .enterpriseProjectName(record.enterprise_project_name())
                .cloudServiceType(record.cloud_service_type())
                .cloudServiceTypeName(record.cloud_service_type_name())
                .skuCode(record.sku_code())
                .productSpecDesc(record.product_spec_desc())
                .resourceTypeCode(record.resource_Type_code())
                .resourceTypeName(record.resource_type_name())
                .resourceName(record.resource_name())
                .region(record.region())
                .regionName(record.region_name())
                .chargeMode(record.charge_mode())
                .consumeAmount(record.consume_amount())
                .officialAmount(record.official_amount())
                .discountAmount(record.discount_amount())
                .couponAmount(record.coupon_amount())
                .build();
    }

}
