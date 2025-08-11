package com.multicloud.batch.model;

import com.multicloud.batch.dao.huawei.payload.HuaweiBillingExpenseResponse;
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
                        "organization_id", "bill_date", "payer_account_id", "customer_id", "enterprise_project_id",
                        "cloud_service_type", "sku_code", "resource_type_code", "region", "charge_mode", "bill_type"
                }
        ),
        indexes = {
                @Index(name = "idx_organization_id", columnList = "organization_id"),
                @Index(name = "idx_bill_date", columnList = "bill_date"),
                @Index(name = "idx_payer_account_id", columnList = "payer_account_id"),
                @Index(name = "idx_customer_id", columnList = "customer_id"),
                @Index(name = "idx_cloud_service_type", columnList = "cloud_service_type"),
                @Index(name = "idx_sku_code", columnList = "sku_code"),
                @Index(
                        name = "idx_project_service",
                        columnList = """
                                    organization_id, bill_date, payer_account_id, customer_id, cloud_service_type, bill_type
                                """
                )
        }
)
public class HuaweiBillingDailyCost {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private long id;

    // Multicloud organization ID
    @Column(name = "organization_id", nullable = false)
    private long organizationId;

    @Column(name = "bill_date", nullable = false)
    private LocalDate billDate;

    // Master/Billing Account ID
    @Column(name = "payer_account_id", nullable = false, length = 64)
    private String payerAccountId;

    // Usage Account ID
    // Usage scope
    @Column(name = "customer_id", nullable = false, length = 64)
    private String customerId;

    @Column(name = "enterprise_project_id")
    private String enterpriseProjectId;
    @Column(name = "enterprise_project_name")
    private String enterpriseProjectName;

    @Column(name = "cloud_service_type", length = 256, nullable = false)
    private String cloudServiceType;
    @Column(name = "cloud_service_type_name", length = 200)
    private String cloudServiceTypeName;

    @Column(name = "sku_code", nullable = false, length = 64)
    private String skuCode;
    @Column(name = "product_spec_desc")
    private String productSpecDesc;

    @Column(name = "resource_type_code", nullable = false, length = 64)
    private String resourceTypeCode;
    @Column(name = "resource_type_name", length = 200)
    private String resourceTypeName;

    // Optional
    @Column(name = "resource_name")
    private String resourceName;

    @Column(nullable = false, length = 64)
    private String region;
    @Column(name = "region_name", length = 64)
    private String regionName;

    // 1: Yearly/monthly
    // 3: Pay-per-use
    // 10: Reserved instances
    @Column(name = "charge_mode", nullable = false)
    private Integer chargeMode;

    // 1: expenditure-purchase
    // 2: expenditure-renewal
    // 3: expenditure-change
    // 4: refund-unsubscription
    // 5: expenditure-use
    // 8: expenditure-auto-renewal
    // 9: adjustment-compensation
    // 14: expenditure-month-end deduction for support plan
    // 15: expenditure-tax
    // 16: adjustment-deduction
    // 17: expenditure-difference amount (min. guaranteed-actual)
    // 20: refund-change
    // 24: refund-changing to Pay-Per-Use
    // 100: refund-unsubscription tax
    // 101: adjustment-tax compensation
    // 102: adjustment-tax deduction
    @Column(name = "bill_type", nullable = false)
    private Integer billType;

    @Column(name = "usage_amount", precision = 30, scale = 8)
    private BigDecimal usageAmount;

    @Column(name = "consume_amount", precision = 20, scale = 8)
    private BigDecimal consumeAmount;
    @Column(name = "official_amount", precision = 20, scale = 8)
    private BigDecimal officialAmount;
    @Column(name = "discount_amount", precision = 20, scale = 8)
    private BigDecimal discountAmount;
    @Column(name = "coupon_amount", precision = 20, scale = 8)
    private BigDecimal couponAmount;

    public static HuaweiBillingDailyCost from(HuaweiResourceBillingResponse.MonthlyRecord record, long orgId) {

        return HuaweiBillingDailyCost.builder()
                .organizationId(orgId)
                .billDate(LocalDate.parse(record.bill_date()))
                .payerAccountId(record.payer_account_id())
                .customerId(record.customer_id())
                .enterpriseProjectId(record.enterprise_project_id())
                .enterpriseProjectName(record.enterprise_project_name())
                .cloudServiceType(record.cloud_service_type())
                .cloudServiceTypeName(record.cloud_service_type_name())
                .skuCode(record.sku_code() == null ? "UNKNOWN" : record.sku_code())
                .productSpecDesc(record.product_spec_desc())
                .resourceTypeCode(record.resource_Type_code())
                .resourceTypeName(record.resource_type_name())
                .resourceName(record.resource_name())
                .region(record.region())
                .regionName(record.region_name())
                .chargeMode(record.charge_mode())
                .billType(record.bill_type())
                .consumeAmount(record.consume_amount())
                .officialAmount(record.official_amount())
                .discountAmount(record.discount_amount())
                .couponAmount(record.coupon_amount())
                .build();
    }


    public static HuaweiBillingDailyCost from(HuaweiBillingExpenseResponse.FeeRecord record, String payerAccountId, long orgId) {

        return HuaweiBillingDailyCost.builder()
                .organizationId(orgId)
                .billDate(LocalDate.parse(record.bill_date()))
                .payerAccountId(payerAccountId)
                .customerId(record.customer_id())
                .enterpriseProjectId(record.enterprise_project_id())
                .enterpriseProjectName(record.enterprise_project_name())
                .cloudServiceType(record.cloud_service_type())
                .cloudServiceTypeName(record.cloud_service_type_name())
                .skuCode(record.sku_code() == null ? "UNKNOWN" : record.sku_code())
                .productSpecDesc(record.product_spec_desc())
                .resourceTypeCode(record.resource_type())
                .resourceTypeName(record.resource_type_name())
                .resourceName(record.resource_name())
                .region(record.region())
                .regionName(record.region_name())
                .chargeMode(Integer.valueOf(record.charge_mode()))
                .consumeAmount(record.amount())
                .officialAmount(record.official_amount())
                .discountAmount(record.discount_amount())
                .couponAmount(record.coupon_amount())
                .build();
    }

}
