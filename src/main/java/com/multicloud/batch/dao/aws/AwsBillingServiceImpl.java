package com.multicloud.batch.dao.aws;

import com.multicloud.batch.cloud_config.aws.AwsDynamicCredentialsProvider;
import com.multicloud.batch.enums.CloudProvider;
import com.multicloud.batch.enums.LastSyncStatus;
import com.multicloud.batch.model.AwsBillingDailyCost;
import com.multicloud.batch.model.CloudDailyBilling;
import com.multicloud.batch.repository.AwsBillingDailyCostRepository;
import com.multicloud.batch.repository.CloudDailyBillingRepository;
import jakarta.persistence.EntityManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.springframework.data.util.Pair;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.athena.model.Datum;
import software.amazon.awssdk.services.athena.model.GetQueryResultsResponse;
import software.amazon.awssdk.services.athena.model.Row;
import software.amazon.awssdk.services.athena.paginators.GetQueryResultsIterable;
import software.amazon.awssdk.services.costexplorer.CostExplorerClient;
import software.amazon.awssdk.services.costexplorer.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
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
public class AwsBillingServiceImpl implements AwsBillingService {

    private final EntityManager entityManager;
    private final CostExplorerClient costExplorerClient;
    private final S3Client s3Client;
    private final AthenaService athenaService;
    private final CloudDailyBillingRepository cloudDailyBillingRepository;
    private final AwsBillingDailyCostRepository awsBillingDailyCostRepository;

    @Override
    public Pair<LastSyncStatus, String> syncDailyServiceCostUsageFromExplorer(long organizationId, String accessKey,
                                                                              String secretKey, boolean firstSync) {

        AwsBasicCredentials credentials = AwsBasicCredentials.create(accessKey, secretKey);
        AwsDynamicCredentialsProvider.setAwsCredentials(credentials);

        try {

            LocalDate start;

            if (firstSync) {
                start = YearMonth.now().minusMonths(12).atDay(1);
            } else {
                start = LocalDate.now().minusDays(7);
            }

            LocalDate end = LocalDate.now();

            GetCostAndUsageRequest request = GetCostAndUsageRequest.builder()
                    .timePeriod(
                            DateInterval.builder()
                                    .start(start.format(DateTimeFormatter.ISO_DATE))
                                    .end(end.format(DateTimeFormatter.ISO_DATE))
                                    .build()
                    )
                    .granularity(Granularity.DAILY)
                    .metrics(Metric.UNBLENDED_COST.name())
                    .groupBy(
                            GroupDefinition.builder().type(GroupDefinitionType.DIMENSION).key(Dimension.LINKED_ACCOUNT.name()).build(),
                            GroupDefinition.builder().type(GroupDefinitionType.DIMENSION).key(Dimension.SERVICE.name()).build()

                    )
                    .build();

            GetCostAndUsageResponse response = costExplorerClient.getCostAndUsage(request);

            List<CloudDailyBilling> billings = new ArrayList<>();

            for (ResultByTime result : response.resultsByTime()) {

                LocalDate startDate = LocalDate.parse(result.timePeriod().start());

                for (Group group : result.groups()) {

                    String accountId = group.keys().getFirst();
                    String service = group.keys().get(1);

                    BigDecimal cost = new BigDecimal(group.metrics().get("UnblendedCost").amount());

                    CloudDailyBilling billing = CloudDailyBilling.builder()
                            .organizationId(organizationId)
                            .cloudProvider(CloudProvider.AWS)
                            .accountId(accountId)
                            .serviceName(service)
                            .date(startDate)
                            .costAmountUsd(cost)
                            .currency("USD")
                            .billingExportSource("AWSCostExplorer")
                            .build();

                    billings.add(billing);

                }

            }

            cloudDailyBillingRepository.batchUpsert(billings, entityManager);

            return Pair.of(LastSyncStatus.SUCCESS, "Successfully synced [%d] items.".formatted(billings.size()));

        } catch (Exception e) {
            log.error("AWS billing data fetch error", e);
            return Pair.of(LastSyncStatus.FAIL, e.getMessage());
        } finally {
            AwsDynamicCredentialsProvider.clear();
        }

    }

    @Override
    public Pair<LastSyncStatus, String> syncDailyCostUsageFromAthena(long organizationId, String accessKey, String secretKey,
                                                                     long days) {

        AwsBasicCredentials credentials = AwsBasicCredentials.create(accessKey, secretKey);
        AwsDynamicCredentialsProvider.setAwsCredentials(credentials);

        try {

            LocalDate date = LocalDate.now().minusDays(days + 1);
            int year = date.getYear();
            int month = date.getMonthValue();

            String query = """
                        SELECT date(line_item_usage_start_date)                            AS usage_date,
                    
                            -- Master/Billing account ID
                            bill_payer_account_id                                          AS payer_account_id,
                    
                            -- Linked/Usage account ID
                            -- Usage Scope
                            line_item_usage_account_id                                     AS usage_account_id,
                    
                            -- Project (from tags)
                            -- User defined tags
                            UPPER(resource_tags_user_project)                              AS project_tag,
                    
                            -- Service
                            product_servicecode                                            AS service_code,
                            product_servicename                                            AS service_name,
                    
                            -- SKU
                            product_sku                                                    AS sku_id,
                            product_description                                            AS sku_description,
                    
                            -- Region & Location
                            product_region                                                 AS region,
                            product_location                                               AS location,
                    
                            -- Currency & Usage & Cost
                            line_item_currency_code                                        AS currency,
                            COALESCE(pricing_term, 'OnDemand')                             AS pricing_type,
                            line_item_usage_type                                           AS usage_type,
                    
                            COALESCE(SUM(line_item_usage_amount), 0)                       AS usage_amount,
                            MAX(pricing_unit)                                              AS usage_unit,
                    
                            COALESCE(SUM(line_item_unblended_cost), 0)                     AS unblended_cost,
                            COALESCE(SUM(line_item_blended_cost), 0)                       AS blended_cost,
                            SUM(
                                COALESCE(reservation_effective_cost, 0) +
                                COALESCE(savings_plan_savings_plan_effective_cost, 0)
                            )                                                              AS effective_cost,
                    
                            -- Billing period
                            MIN(bill_billing_period_start_date)                            AS billing_period_start,
                            MAX(bill_billing_period_end_date)                              AS billing_period_end
                    
                        FROM %s
                        WHERE (CAST(year AS INTEGER) > %d OR (CAST(year AS INTEGER) = %d AND CAST(month AS INTEGER) >= %d))
                            AND line_item_usage_start_date >= date_add('day', -%d, current_date)
                            AND line_item_line_item_type IN (
                                'Usage', 'DiscountedUsage', 'SavingsPlanCoveredUsage', 'SavingsPlanNegation',
                                'SavingsPlanRecurringFee', 'RIFee', 'EdpDiscount', 'Tax', 'Support', 'Refund',
                                'Credit', 'Fee', 'Rounding'
                             )
                            AND line_item_unblended_cost IS NOT NULL
                        GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13
                        ORDER BY 1 DESC;
                    """.formatted("athena", year, year, month, days);

            String bucket = "azerion-athena-results";
            String prefix = "azerion_mc";
            String outputLocation = "s3://%s/%s/".formatted(bucket, prefix);
            String database = "athenacurcfn_athena";

            String executionId = athenaService.submitAthenaQuery(query, outputLocation, database);
            athenaService.waitForQueryToComplete(executionId);

            long totalResults = fetchResultsToUnloadedFileFromS3(organizationId, bucket, prefix, executionId);
//            long totalResults = fetchQueryResultsAndSaveItIntoDB(organizationId, executionId);

//            athenaService.printQueryResults(executionId);

            log.info("AWS billing data fetched and stored successfully. Total results: {}", totalResults);

            return Pair.of(LastSyncStatus.SUCCESS, "Successfully synced [%d] items.".formatted(totalResults));
        } catch (Exception e) {
            log.error("AWS billing data fetch error", e);
            return Pair.of(LastSyncStatus.FAIL, e.getMessage());
        } finally {
            AwsDynamicCredentialsProvider.clear();
        }

    }

//    @Override
//    public void fetchDailyServiceCostUsage(String accessKey, String secretKey, long organizationId) {
//
//        AwsBasicCredentials credentials = AwsBasicCredentials.create(accessKey, secretKey);
//        AwsDynamicCredentialsProvider.setAwsCredentials(credentials);
//
//        try {
//
//            LocalDateTime now = LocalDateTime.now();
//
//            LocalDate start = YearMonth.now().minusMonths(12).atDay(1);
//            LocalDate end = LocalDate.now();
//
//            GetCostAndUsageRequest request = GetCostAndUsageRequest.builder()
//                    .timePeriod(
//                            DateInterval.builder()
//                                    .start(start.format(DateTimeFormatter.ISO_DATE))
//                                    .end(end.format(DateTimeFormatter.ISO_DATE))
//                                    .build()
//                    )
//                    .granularity(Granularity.DAILY)
//                    .metrics(Metric.UNBLENDED_COST.name(), Metric.USAGE_QUANTITY.name())
//                    .groupBy(
//                            GroupDefinition.builder().type(GroupDefinitionType.DIMENSION).key(Dimension.SERVICE.name()).build(),
//                            GroupDefinition.builder().type(GroupDefinitionType.DIMENSION).key(Dimension.USAGE_TYPE.name()).build()
//
//                    )
//                    .build();
//
//            GetCostAndUsageResponse response = costExplorerClient.getCostAndUsage(request);
//
//            List<CloudBilling> billings = new ArrayList<>();
//
//            for (ResultByTime result : response.resultsByTime()) {
//
//                LocalDate startDate = LocalDate.parse(result.timePeriod().start());
//                LocalDate endDate = LocalDate.parse(result.timePeriod().end());
//
//                for (Group group : result.groups()) {
//
//                    String service = group.keys().getFirst();
//                    String usageType = group.keys().get(1);
//
//                    BigDecimal usage = new BigDecimal(group.metrics().get("UsageQuantity").amount());
//                    BigDecimal cost = new BigDecimal(group.metrics().get("UnblendedCost").amount());
//
//                    CloudBilling billing = CloudBilling.builder()
//                            .organizationId(1)
//                            .cloudProvider(CloudProvider.AWS)
//                            .serviceName(service)
//                            .skuName(usageType)
//                            .usageStartDate(startDate)
//                            .usageEndDate(endDate)
//                            .usageAmount(usage)
//                            .usageUnit(null)
//                            .costAmountUsd(cost)
//                            .currency("USD")
//                            .billingExportSource("AWSCostExplorer")
//                            .build();
//
//                    billings.add(billing);
//
//                }
//
//            }
//
//            System.out.println("=============================================");
//
//            System.out.println("Fetch time: " + Duration.between(now, LocalDateTime.now()).toSeconds());
//
//            now = LocalDateTime.now();
//
//            System.out.println("Total count: " + billings.size());
//            cloudBillingRepository.saveAll(billings);
//
//            System.out.println("Save time: " + Duration.between(now, LocalDateTime.now()).toSeconds());
//
//            System.out.println("=============================================");
//
//        } finally {
//            AwsDynamicCredentialsProvider.clear();
//        }
//
//    }

    @Override
    public boolean checkAwsExplorerConnection(String accessKey, String secretKey) {

        AwsBasicCredentials credentials = AwsBasicCredentials.create(accessKey, secretKey);
        AwsDynamicCredentialsProvider.setAwsCredentials(credentials);

        try {

            LocalDate end = LocalDate.now();
            LocalDate start = end.minusDays(1);

            GetCostAndUsageRequest request = GetCostAndUsageRequest.builder()
                    .timePeriod(DateInterval.builder()
                            .start(start.format(DateTimeFormatter.ISO_DATE))
                            .end(end.format(DateTimeFormatter.ISO_DATE))
                            .build())
                    .granularity(Granularity.DAILY)
                    .metrics("BlendedCost")
                    .build();

            costExplorerClient.getCostAndUsage(request); // If this doesn't throw, access is working
            costExplorerClient.close();

            return true;
        } catch (Exception e) {
            log.error("Cost Explorer error: {}", e.getMessage());
            return false;
        } finally {
            AwsDynamicCredentialsProvider.clear();
        }

    }

    public long fetchQueryResultsAndSaveItIntoDB(long organizationId, String executionId) {

        log.info("Start fetching query results.");

        int count = 0;

        List<AwsBillingDailyCost> results = new ArrayList<>();

        GetQueryResultsIterable resultsPages = athenaService.fetchQueryResults(executionId);

        log.info("Fetched pagination results.");

        boolean hasHeader = true;

        for (GetQueryResultsResponse response : resultsPages) {

            List<Row> rows = response.resultSet().rows();

            int i = 0;

            if (hasHeader) {
                i = 1;
                hasHeader = false;
            }

            for (; i < rows.size(); i++) {
                results.add(bindRow(rows.get(i), organizationId));
                count++;

            }

            // Save each batch
            // Clean before the next
            log.info("Upserting {} fetched AWS records into DB.", results.size());
            awsBillingDailyCostRepository.upsertAwsBillingDailyCosts(results, entityManager);
            results.clear();

        }

        return count;
    }

    private long fetchResultsToUnloadedFileFromS3(long organizationId, String bucket, String prefix, String executionId) {

        ListObjectsV2Request s3Request = ListObjectsV2Request.builder()
                .bucket(bucket)
                .prefix(prefix)
                .build();

        ListObjectsV2Response s3Response = s3Client.listObjectsV2(s3Request);

        List<AwsBillingDailyCost> results = new ArrayList<>();
        long count = 0;

        for (S3Object s3Object : s3Response.contents()) {

            String key = s3Object.key();

            // Only process files with this execution ID and CSV extension
            if (key.contains(executionId) && key.endsWith(".csv")) {

                log.info("Processing AWS Athena unloaded file: {}", key);

                // Read and parse this file only
                GetObjectRequest getRequest = GetObjectRequest.builder()
                        .bucket(bucket)
                        .key(key)
                        .build();

                try (ResponseInputStream<GetObjectResponse> s3Stream = s3Client.getObject(getRequest);
                     BufferedReader reader = new BufferedReader(new InputStreamReader(s3Stream))) {

                    Iterable<CSVRecord> records = CSVFormat.DEFAULT
                            .builder()
                            .setHeader()
                            .setSkipHeaderRecord(true)
                            .get()
                            .parse(reader);

                    for (CSVRecord record : records) {

                        results.add(bindRecord(record, organizationId));
                        count++;

                        if (results.size() == 20000) {
                            // Save each batch
                            // Clean before the next
                            log.info("Upserting {} fetched AWS records into DB.", results.size());
                            awsBillingDailyCostRepository.upsertAwsBillingDailyCosts(results, entityManager);
                            results.clear();
                        }

                    }

                } catch (IOException e) {
                    log.error("Error reading file: {}", key, e);
                }

            }

        }

        // Save the remaining records
        log.info("Upserting {} fetched AWS records into DB.", results.size());
        awsBillingDailyCostRepository.upsertAwsBillingDailyCosts(results, entityManager);
        results.clear();

        return count;
    }

    private AwsBillingDailyCost bindRow(Row row, long organizationId) {

        List<Datum> data = row.data();

        AwsBillingDailyCost dailyCost = new AwsBillingDailyCost();
        dailyCost.setOrganizationId(organizationId);

        dailyCost.setUsageDate(LocalDate.parse(data.get(0).varCharValue()));
        dailyCost.setPayerAccountId(data.get(1).varCharValue());
        dailyCost.setUsageAccountId(data.get(2).varCharValue());

        dailyCost.setProjectTag(data.get(3).varCharValue());

        dailyCost.setServiceCode(data.get(4).varCharValue());
        dailyCost.setServiceName(data.get(5).varCharValue());
        dailyCost.setSkuId(data.get(6).varCharValue());
        dailyCost.setSkuDescription(data.get(7).varCharValue());
        dailyCost.setRegion(data.get(8).varCharValue());
        dailyCost.setLocation(data.get(9).varCharValue());
        dailyCost.setCurrency(data.get(10).varCharValue());
        dailyCost.setPricingType(data.get(11).varCharValue());
        dailyCost.setUsageType(data.get(12).varCharValue());

        dailyCost.setUsageAmount(parseBigDecimalSafe(data.get(13).varCharValue()));
        dailyCost.setUsageUnit(data.get(14).varCharValue());
        dailyCost.setUnblendedCost(parseBigDecimalSafe(data.get(15).varCharValue()));
        dailyCost.setBlendedCost(parseBigDecimalSafe(data.get(16).varCharValue()));
        dailyCost.setEffectiveCost(parseBigDecimalSafe(data.get(17).varCharValue()));

        dailyCost.setBillingPeriodStart(
                LocalDateTime.parse(data.get(18).varCharValue().replace(" ", "T"))
        );
        dailyCost.setBillingPeriodEnd(
                LocalDateTime.parse(data.get(19).varCharValue().replace(" ", "T"))
        );

        return dailyCost;
    }

    private AwsBillingDailyCost bindRecord(CSVRecord record, long organizationId) {

        return AwsBillingDailyCost.builder()
                .organizationId(organizationId)
                .usageDate(LocalDate.parse(record.get("usage_date")))
                .payerAccountId(record.get("payer_account_id"))
                .usageAccountId(record.get("usage_account_id"))
                .projectTag(record.get("project_tag"))
                .serviceCode(record.get("service_code"))
                .serviceName(record.get("service_name"))
                .skuId(record.get("sku_id"))
                .skuDescription(record.get("sku_description"))
                .region(record.get("region"))
                .location(record.get("location"))
                .currency(record.get("currency"))
                .pricingType(record.get("pricing_type"))
                .usageType(record.get("usage_type"))
                .usageAmount(parseBigDecimalSafe(record.get("usage_amount")))
                .usageUnit(record.get("usage_unit"))
                .unblendedCost(parseBigDecimalSafe(record.get("unblended_cost")))
                .blendedCost(parseBigDecimalSafe(record.get("blended_cost")))
                .effectiveCost(parseBigDecimalSafe(record.get("effective_cost")))
                .billingPeriodStart(
                        LocalDateTime.parse(
                                record.get("billing_period_start").replace(" ", "T")
                        )
                )
                .billingPeriodEnd(
                        LocalDateTime.parse(
                                record.get("billing_period_end").replace(" ", "T")
                        )
                )
                .build();
    }

    private BigDecimal parseBigDecimalSafe(String value) {
        return (value == null || value.isEmpty()) ? BigDecimal.ZERO : new BigDecimal(value);
    }

}
