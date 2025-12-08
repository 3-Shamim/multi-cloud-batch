package com.multicloud.batch.secondary.model;

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
        name = "aws_customer_daily_cost",
        uniqueConstraints = {
                @UniqueConstraint(name = "idx_uq_const", columnNames = {"day", "mc_org_id", "customer_name"})
        }
)
public class AwsCustomerDailyCost {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private long id;

    @Column(nullable = false)
    private LocalDate day;

    @Column(name = "mc_org_id", nullable = false)
    private long mcOrgId;

    @Column(name = "mc_org_name", nullable = false, length = 100)
    private String mcOrgName;

    @Column(name = "customer_name", nullable = false, length = 100)
    private String customerName;

    @Column(name = "azerion_cost", precision = 20, scale = 8)
    private BigDecimal azerionCost;

    @Column(name = "customer_cost", precision = 20, scale = 8)
    private BigDecimal customerCost;

    @Column(nullable = false, columnDefinition = "boolean default false")
    private boolean external;

}
