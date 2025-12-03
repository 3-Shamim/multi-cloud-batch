package com.multicloud.batch.model;

import com.multicloud.batch.enums.CloudProvider;
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
        name = "service_level_billings",
        uniqueConstraints = @UniqueConstraint(
                name = "idx_uq_const_slb",
                columnNames = {
                        "usage_date", "cloud_provider", "billing_account_id", "usage_account_id",
                        "service_code", "billing_type", "is_li_outside_of_month"
                }
        ),
        indexes = {
                @Index(name = "idx_usage_date", columnList = "usage_date"),
                @Index(name = "idx_cloud_provider", columnList = "cloud_provider"),
                @Index(name = "idx_payer_account_id", columnList = "billing_account_id"),
                @Index(name = "idx_usage_account_id", columnList = "usage_account_id"),
                @Index(name = "idx_service_code", columnList = "service_code"),
                @Index(name = "idx_billing_type", columnList = "billing_type"),
                @Index(
                        name = "idx_slb_account_date",
                        columnList = "usage_account_id, usage_date"
                ),
                @Index(
                        name = "idx_slb_account_date_provider",
                        columnList = "usage_account_id, usage_date, cloud_provider"
                )
        }
)
public class ServiceLevelBilling {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "usage_date", nullable = false)
    private LocalDate usageDate;

    @Enumerated(EnumType.STRING)
    @Column(name = "cloud_provider", nullable = false, length = 100)
    private CloudProvider cloudProvider;

    // Master/Billing Account ID
    @Column(name = "billing_account_id", nullable = false, length = 64)
    private String billingAccountId;

    // Linked/Usage Account ID
    // Usage scope
    @Column(name = "usage_account_id", nullable = false, length = 64)
    private String usageAccountId;

    @Column(name = "usage_account_name", length = 128)
    private String usageAccountName;

    @Column(name = "service_code", nullable = false, length = 256)
    private String serviceCode;

    @Column(name = "service_name", length = 200)
    private String serviceName;

    @Column(name = "billing_type", nullable = false, length = 32)
    private String billingType;

    @Column(name = "is_li_outside_of_month", nullable = false, columnDefinition = "boolean default false")
    private boolean isLiOutsideOfMonth;

    @Column(name = "parent_category", length = 200)
    private String parentCategory;

    // Represent internal cost
    @Column(precision = 20, scale = 8)
    private BigDecimal cost;

    // Represent external cost
    @Column(name = "ext_cost", precision = 20, scale = 8)
    private BigDecimal extCost;



}
