package com.multicloud.batch.model;

import com.multicloud.batch.enums.CloudProvider;
import jakarta.persistence.*;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serial;
import java.io.Serializable;
import java.math.BigDecimal;
import java.time.LocalDate;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim Molla
 * Email: shamim.molla@vivasoftltd.com
 */

@Entity
@Table(
        name = "cloud_daily_billings",
        indexes = {
                @Index(
                        name = "cloud_daily_billings_compound_idx",
                        columnList = "organizationId, cloudProvider, accountId, projectId, serviceName, date",
                        unique = true
                )
        }
)
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CloudDailyBilling implements Serializable {

    @Serial
    private static final long serialVersionUID = 7985489713468541244L;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Positive(message = "Organization ID must be a positive number")
    @Column(nullable = false)
    private long organizationId;

    @NotNull(message = "Cloud provider must not be null")
    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 25)
    private CloudProvider cloudProvider;

    @Column(name = "account_id")
    private String accountId;

    @Column(name = "project_id")
    private String projectId;

    @Column(name = "service_name")
    private String serviceName;

    @Column(name = "date")
    private LocalDate date;

    @Column(name = "cost_amount_usd", precision = 20, scale = 6)
    private BigDecimal costAmountUsd;

    @Column(name = "currency")
    private String currency;

    @Column(name = "billing_export_source")
    private String billingExportSource;

}