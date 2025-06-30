package com.multicloud.batch.job.gcp;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDate;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim Molla
 * Email: shamim.molla@vivasoftltd.com
 */

@Data
@AllArgsConstructor
@NoArgsConstructor
public class BillingRecord {

    private String accountId;
    private String projectId;
    private String service;
    private LocalDate date;
    private BigDecimal cost;

}