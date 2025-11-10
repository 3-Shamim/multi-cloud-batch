package com.multicloud.batch.dao.google;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.*;
import com.multicloud.batch.model.GcpBillingDailyCost;
import com.multicloud.batch.repository.GcpBillingDailyCostRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.io.ByteArrayInputStream;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim Molla
 * Email: shamim.molla@vivasoftltd.com
 */

@Slf4j
@RequiredArgsConstructor
@Service
public class GoogleBillingServiceImpl implements GoogleBillingService {

    private final JdbcTemplate jdbcTemplate;
    private final GcpBillingDailyCostRepository gcpBillingDailyCostRepository;

    @Override
    public void fetchDailyServiceCostUsage(byte[] jsonKey, LocalDate start, LocalDate end, boolean internal) {

        String query = """
                    SELECT DATE(t.usage_start_time)                                      AS usage_date,
                
                        MAX(PARSE_DATE('%Y%m', invoice.month))                           AS billing_month,
                
                        -- Billing account ID
                        t.billing_account_id                                             AS billing_account_id,
                
                        -- Project Info
                        -- Usage Scop
                        COALESCE(t.project.id, 'UNKNOWN')                                AS project_id,
                        COALESCE(MAX(t.project.name), 'UNKNOWN')                         AS project_name,
                
                        -- Service
                        COALESCE(t.service.id, 'UNKNOWN')                                AS service_code,
                        COALESCE(MAX(t.service.description), 'UNKNOWN')                  AS service_name,
                
                        -- SKU
                        COALESCE(t.sku.id, 'UNKNOWN')                                    AS sku_id,
                        COALESCE(MAX(t.sku.description), 'UNKNOWN')                      AS sku_description,
                
                        -- Region & Location
                        COALESCE(t.location.region, 'UNKNOWN')                           AS region,
                        COALESCE(MAX(t.location.location), 'UNKNOWN')                    AS location,
                
                        -- Currency & Usage & Cost
                        COALESCE(MAX(t.currency), 'UNKNOWN')                             AS currency,
                
                        COALESCE(t.cost_type, 'UNKNOWN')                                 AS cost_type,
                
                        ROUND(COALESCE(SUM(t.usage.amount), 0), 8)                       AS usage_amount,
                        COALESCE(MAX(t.usage.unit), 'UNKNOWN')                           AS usage_unit,
                
                        ROUND(COALESCE(SUM(t.cost), 0), 8)                               AS cost,
                        ROUND(
                            SUM((SELECT COALESCE(SUM(c.amount), 0) FROM UNNEST(t.credits) AS c)),
                            8
                        )                                                                AS credits
                
                    FROM `azerion-billing.azerion_billing_eu.gcp_billing_export_v1_*` AS t
                    WHERE _PARTITIONDATE BETWEEN ':start_date' AND ':end_date'
                    GROUP BY 1, 3, 4, 6, 8, 10, 13
                """;

        query = query
                .replace(":start_date", start.format(DateTimeFormatter.ISO_DATE))
                .replace(":end_date", end.format(DateTimeFormatter.ISO_DATE));

        try {

            GoogleCredentials credentials = GoogleCredentials.fromStream(
                    new ByteArrayInputStream(jsonKey)
            );

            BigQuery bigQuery = BigQueryOptions.newBuilder()
                    .setCredentials(credentials)
                    .build()
                    .getService();

            QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query).build();

            TableResult result = bigQuery.query(queryConfig);

            List<GcpBillingDailyCost> billings = new ArrayList<>();

            long count = 0;

            // BigQuery returns results in pages.
            // Each page can contain up to 10,000 rows by default, depending on the query and row size.
            for (FieldValueList row : result.iterateAll()) {

                GcpBillingDailyCost billing = bindRow(row, internal);

                billings.add(billing);
                count++;

                if (billings.size() == 5000) {

                    // Save each batch
                    // Clean before the next
                    log.info("Upserting {} fetched GCP records into DB.", billings.size());
                    gcpBillingDailyCostRepository.upsertGcpBillingDailyCosts(billings, jdbcTemplate, internal);
                    billings.clear();

                }

            }

            log.info("Upserting {} fetched GCP records into DB.", billings.size());
            gcpBillingDailyCostRepository.upsertGcpBillingDailyCosts(billings, jdbcTemplate, internal);
            billings.clear();

            log.info("GCP billing data fetched and stored successfully. Total results: {}", count);

        } catch (Exception e) {
            log.error("GCP billing data fetch error", e);
            throw new RuntimeException("GCP billing data fetch error", e);
        }

    }

    private GcpBillingDailyCost bindRow(FieldValueList row, boolean internal) {

        return GcpBillingDailyCost.builder()
                .usageDate(parseLocalDate(row, "usage_date"))
                .billingMonth(parseLocalDate(row, "billing_month"))
                .billingAccountId(getStringSafe(row, "billing_account_id"))
                .projectId(getStringSafe(row, "project_id"))
                .projectName(getStringSafe(row, "project_name"))
                .serviceCode(getStringSafe(row, "service_code"))
                .serviceName(getStringSafe(row, "service_name"))
                .skuId(getStringSafe(row, "sku_id"))
                .skuDescription(getStringSafe(row, "sku_description"))
                .region(getStringSafe(row, "region"))
                .location(getStringSafe(row, "location"))
                .currency(getStringSafe(row, "currency"))
                .costType(getStringSafe(row, "cost_type"))
                .usageAmount(getNumericSafe(row, "usage_amount"))
                .usageUnit(getStringSafe(row, "usage_unit"))
                .cost(internal ? getNumericSafe(row, "cost") : BigDecimal.ZERO)
                .credits(internal ? getNumericSafe(row, "credits") : BigDecimal.ZERO)
                .extCost(internal ? BigDecimal.ZERO : getNumericSafe(row, "cost"))
                .extCredits(internal ? BigDecimal.ZERO : getNumericSafe(row, "credits"))
                .build();
    }

    private String getStringSafe(FieldValueList row, String fieldName) {
        FieldValue field = row.get(fieldName);
        return field.isNull() ? null : field.getStringValue();
    }

    private BigDecimal getNumericSafe(FieldValueList row, String fieldName) {
        FieldValue field = row.get(fieldName);
        return field.isNull() ? null : field.getNumericValue();
    }

    private LocalDate parseLocalDate(FieldValueList row, String fieldName) {
        String value = getStringSafe(row, fieldName);
        return value != null ? LocalDate.parse(value) : null;
    }

    private LocalDateTime parseLocalDateTime(FieldValueList row, String fieldName) {
        String value = getStringSafe(row, fieldName);
        return value != null ? LocalDateTime.parse(value) : null;
    }

}
