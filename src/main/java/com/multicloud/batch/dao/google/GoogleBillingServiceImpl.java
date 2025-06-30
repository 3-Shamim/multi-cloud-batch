package com.multicloud.batch.dao.google;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.*;
import com.multicloud.batch.enums.CloudProvider;
import com.multicloud.batch.enums.LastSyncStatus;
import com.multicloud.batch.model.CloudDailyBilling;
import com.multicloud.batch.repository.CloudDailyBillingRepository;
import jakarta.persistence.EntityManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.util.Pair;
import org.springframework.stereotype.Service;

import java.io.ByteArrayInputStream;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.YearMonth;
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

    private final EntityManager entityManager;
    private final CloudDailyBillingRepository cloudDailyBillingRepository;

    @Override
    public Pair<LastSyncStatus, String> fetchDailyServiceCostUsage(long organizationId, byte[] jsonKey, LastSyncStatus lastSyncStatus) {

        LocalDate start;

        if (lastSyncStatus == null) {
            start = YearMonth.now().minusMonths(12).atDay(1);
        } else {
            start = LocalDate.now().minusDays(7);
        }

        LocalDate end = LocalDate.now();

        String query = """
                    SELECT
                        DATE(usage_start_time) AS start_date,
                        billing_account_id AS account_id,
                        project.id AS project_id,
                        service.description AS service_name,
                        SUM(cost) AS cost_amount_usd,
                        ANY_VALUE(currency) AS currency
                    FROM
                        `azerion-billing.azerion_billing_eu.gcp_billing_export_v1_*`
                    WHERE
                        usage_start_time >= ':start_date' AND usage_start_time < ':end_date'
                    GROUP BY
                        start_date, account_id, project_id, service_name
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

            List<CloudDailyBilling> billings = new ArrayList<>();

            long totalCount = 0;

            for (FieldValueList row : result.iterateAll()) {

                CloudDailyBilling billing = CloudDailyBilling.builder()
                        .organizationId(organizationId)
                        .cloudProvider(CloudProvider.GCP)
                        .accountId(row.get("account_id").isNull() ? null : row.get("account_id").getStringValue())
                        .projectId(row.get("project_id").isNull() ? null : row.get("project_id").getStringValue())
                        .serviceName(row.get("service_name").getStringValue())
                        .date(LocalDate.parse(row.get("start_date").getStringValue()))
                        .costAmountUsd(row.get("cost_amount_usd").isNull() ? null : BigDecimal.valueOf(row.get("cost_amount_usd").getDoubleValue()))
                        .currency(row.get("currency").isNull() ? null : row.get("currency").getStringValue())
                        .billingExportSource("BigQueryBillingExport")
                        .build();

                billings.add(billing);

                if (billings.size() == 20000) {

                    totalCount += saveDailyBillingIntoDB(billings);

                    billings.clear();

                }

            }

            totalCount += saveDailyBillingIntoDB(billings);

            return Pair.of(LastSyncStatus.SUCCESS, "Successfully synced [%d] items.".formatted(totalCount));

        } catch (Exception e) {
            log.error("GCP billing data fetch error", e);
            return Pair.of(LastSyncStatus.FAIL, e.getMessage());
        }

    }

//    @Override
//    public void fetchDailyServiceCostUsage(byte[] jsonKey, long organizationId) {
//
//        LocalDateTime now = LocalDateTime.now();
//
//        LocalDate start = YearMonth.now().minusMonths(12).atDay(1);
//        LocalDate end = LocalDate.now();
//
//        String query = """
//                    SELECT
//                        DATE(usage_start_time) AS start_date,
//                        DATE(usage_end_time) AS end_date,
//                        billing_account_id AS account_id,
//                        project.id AS project_id,
//                        service.description AS service_name,
//                        sku.description AS sku_name,
//                        SUM(usage.amount) AS usage_amount,
//                        ANY_VALUE(usage.unit) AS usage_unit,
//                        SUM(cost) AS cost_amount_usd,
//                        ANY_VALUE(currency) AS currency
//                    FROM
//                        `azerion-billing.azerion_billing_eu.gcp_billing_export_v1_*`
//                    WHERE
//                        usage_start_time >= ':start_date' AND usage_start_time < ':end_date'
//                    GROUP BY
//                        start_date, end_date, account_id, project_id, service_name, sku_name
//                """;
//
//        query = query
//                .replace(":start_date", start.format(DateTimeFormatter.ISO_DATE))
//                .replace(":end_date", end.format(DateTimeFormatter.ISO_DATE));
//
//        try {
//
//            GoogleCredentials credentials = GoogleCredentials.fromStream(
//                    new ByteArrayInputStream(jsonKey)
//            );
//
//            BigQuery bigQuery = BigQueryOptions.newBuilder()
//                    .setCredentials(credentials)
//                    .build()
//                    .getService();
//
//            QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query).build();
//            TableResult result = bigQuery.query(queryConfig);
//
//            System.out.println("Fetch time: " + java.time.Duration.between(now, LocalDateTime.now()).toSeconds());
//
//            List<CloudBilling> billings = new ArrayList<>();
//
//            long totalCount = 0;
//
//            for (FieldValueList row : result.iterateAll()) {
//
//                CloudBilling billing = CloudBilling.builder()
//                        .organizationId(1) // for test
//                        .cloudProvider(CloudProvider.GCP)
//                        .accountId(row.get("account_id").isNull() ? null : row.get("account_id").getStringValue())
//                        .projectId(row.get("project_id").isNull() ? null : row.get("project_id").getStringValue())
//                        .serviceName(row.get("service_name").getStringValue())
//                        .skuName(row.get("sku_name").getStringValue())
//                        .usageStartDate(LocalDate.parse(row.get("start_date").getStringValue()))
//                        .usageEndDate(LocalDate.parse(row.get("end_date").getStringValue())) // Same for daily granularity
//                        .usageAmount(row.get("usage_amount").isNull() ? null : BigDecimal.valueOf(row.get("usage_amount").getDoubleValue()))
//                        .usageUnit(row.get("usage_unit").isNull() ? null : row.get("usage_unit").getStringValue())
//                        .costAmountUsd(row.get("cost_amount_usd").isNull() ? null : BigDecimal.valueOf(row.get("cost_amount_usd").getDoubleValue()))
//                        .currency(row.get("currency").isNull() ? null : row.get("currency").getStringValue())
//                        .billingExportSource("BigQueryBillingExport")
//                        .build();
//
//                billings.add(billing);
//
//                if (billings.size() == 20000) {
//
//                    totalCount += saveIntoDB(billings);
//
//                    billings.clear();
//
//                }
//
//            }
//
//            totalCount += saveIntoDB(billings);
//
//            System.out.println("Total count: " + totalCount);
//
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//
//
//    }

    private int saveDailyBillingIntoDB(List<CloudDailyBilling> billings) {
        cloudDailyBillingRepository.batchUpsert(billings, entityManager);
        return billings.size();
    }

    @Override
    public boolean checkGoogleBigQueryConnection(byte[] jsonKey) {

        try {

            GoogleCredentials credentials = GoogleCredentials
                    .fromStream(new ByteArrayInputStream(jsonKey));

            BigQuery bigQuery = BigQueryOptions.newBuilder()
                    .setCredentials(credentials)
                    .build()
                    .getService();

            String query = "SELECT 1 FROM `azerion-billing.azerion_billing_eu.gcp_billing_export_v1_*` LIMIT 1";

            QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query).build();
            bigQuery.query(queryConfig);

            return true;
        } catch (Exception e) {
            log.error("Google BigQuery Error: {}", e.getMessage());
            return false;
        }

    }

}
