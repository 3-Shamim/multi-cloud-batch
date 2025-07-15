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
import org.springframework.data.util.Pair;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.core.pagination.sync.SdkIterable;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.athena.model.*;
import software.amazon.awssdk.services.costexplorer.CostExplorerClient;
import software.amazon.awssdk.services.costexplorer.model.*;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.YearMonth;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
    private final AthenaClient athenaClient;
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
                        SELECT date(line_item_usage_start_date)                               AS usage_date,
                    
                               -- Account
                               bill_payer_account_id                                          AS payer_account_id,
                               line_item_usage_account_id                                     AS usage_account_id,
                    
                               -- Project (from tags)
                               resource_tags_user_project                                     AS project_id,
                    
                               -- Service
                               product_servicecode                                            AS service_code,
                               product_servicename                                            AS service_name,
                    
                               -- SKU
                               product_sku                                                    AS sku_id,
                               product_description                                            AS sku_description,
                    
                               -- Region / Location
                               product_region                                                 AS region,
                               product_location                                               AS location,
                    
                               line_item_currency_code                                        AS currency,
                               COALESCE(pricing_term, 'OnDemand')                             AS pricing_type,
                               line_item_usage_type                                           AS usage_type,
                    
                               SUM(line_item_usage_amount)                                    AS usage_amount,
                               MAX(pricing_unit)                                              AS usage_unit,
                    
                               SUM(line_item_unblended_cost)                                  AS unblended_cost,
                               SUM(line_item_blended_cost)                                    AS blended_cost,
                               SUM(
                                       COALESCE(reservation_effective_cost, 0) +
                                       COALESCE(savings_plan_savings_plan_effective_cost, 0)
                               )                                                              AS effective_cost,
                    
                               MIN(bill_billing_period_start_date)                            AS billing_period_start,
                               MAX(bill_billing_period_end_date)                              AS billing_period_end
                    
                        FROM %s
                        WHERE (CAST(year AS INTEGER) > %d OR (CAST(year AS INTEGER) = %d AND CAST(month AS INTEGER) >= %d))
                            AND line_item_usage_start_date >= date_add('day', -%d, current_date)
                            AND line_item_line_item_type IN ('Usage', 'DiscountedUsage', 'SavingsPlanCoveredUsage')
                            AND line_item_unblended_cost IS NOT NULL
                        GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13
                        ORDER BY 1 DESC;
                    """.formatted("athena", year, year, month, days);

            String executionId = submitAthenaQuery(query);
            waitForQueryToComplete(executionId);

            long totalResults = fetchQueryResultsAndSaveItIntoDB(executionId);
//            List<Map<String, String>> results = getQueryResults(executionId);
//            results.forEach(System.out::println);

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

    private String submitAthenaQuery(String query) {

        StartQueryExecutionRequest request = StartQueryExecutionRequest.builder()
                .queryString(query)
                .queryExecutionContext(
                        QueryExecutionContext.builder().database("athenacurcfn_athena").build()
                )
                .resultConfiguration(
                        ResultConfiguration.builder().outputLocation("s3://azerion-athena-results/azerion_mc/").build()
                )
                .build();

        StartQueryExecutionResponse response = athenaClient.startQueryExecution(request);

        log.info("Athena query submitted successfully.");

        return response.queryExecutionId();
    }

    public void waitForQueryToComplete(String executionId) throws InterruptedException {

        while (true) {

            GetQueryExecutionRequest getRequest = GetQueryExecutionRequest.builder()
                    .queryExecutionId(executionId)
                    .build();

            QueryExecution queryExecution = athenaClient.getQueryExecution(getRequest)
                    .queryExecution();

            QueryExecutionStatus status = queryExecution.status();

            QueryExecutionState state = status.state();

            if (state == QueryExecutionState.SUCCEEDED) {
                log.info("Athena query execution completed successfully.");
                return;
            }

            if (state == QueryExecutionState.FAILED || state == QueryExecutionState.CANCELLED) {
                log.error("Athena query execution failed.");
                throw new RuntimeException(status.stateChangeReason());
            }

            Thread.sleep(1000); // Poll every second
        }

    }

    public List<Map<String, String>> getQueryResults(String executionId) {

        List<Map<String, String>> results = new ArrayList<>();

        String nextToken = null;

        do {

            GetQueryResultsRequest resultRequest = GetQueryResultsRequest.builder()
                    .queryExecutionId(executionId)
                    .nextToken(nextToken)
                    .build();

            GetQueryResultsResponse resultResponse = athenaClient.getQueryResults(resultRequest);

            List<ColumnInfo> columnInfoList = resultResponse.resultSet().resultSetMetadata().columnInfo();

            List<Row> rows = resultResponse.resultSet().rows();

            int i = 0;

            if (nextToken == null) {
                i = 1; // skip header
            }

            for (; i < rows.size(); i++) {

                Row row = rows.get(i);

                Map<String, String> rowMap = new HashMap<>();

                for (int j = 0; j < row.data().size(); j++) {
                    rowMap.put(columnInfoList.get(j).name(), row.data().get(j).varCharValue());
                }

                results.add(rowMap);

            }

            nextToken = resultResponse.nextToken();

        } while (nextToken != null);

        return results;
    }

    public long fetchQueryResultsAndSaveItIntoDB(String executionId) {

        log.info("Start fetching query results.");

        int count = 0;

        List<AwsBillingDailyCost> results = new ArrayList<>();

        GetQueryResultsRequest request = GetQueryResultsRequest.builder()
                .queryExecutionId(executionId)
                .build();

        SdkIterable<GetQueryResultsResponse> resultsPages = athenaClient.getQueryResultsPaginator(request);

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
                results.add(bindRow(rows.get(i)));
            }

            log.info("Upserting {} fetched records into DB.", results.size());

            count += results.size();

            // Save each batch
            awsBillingDailyCostRepository.upsertAwsBillingDailyCosts(results, entityManager);

            // Clean before the next
            results.clear();

        }

        return count;
    }

    public AwsBillingDailyCost bindRow(Row row) {

        List<Datum> data = row.data();

        AwsBillingDailyCost usage = new AwsBillingDailyCost();

        usage.setUsageDate(LocalDate.parse(data.get(0).varCharValue()));
        usage.setPayerAccountId(data.get(1).varCharValue());
        usage.setUsageAccountId(data.get(2).varCharValue());

        String projectId = data.get(3).varCharValue();
        usage.setProjectId(projectId);
        usage.setProjectName(projectId);

        usage.setServiceCode(data.get(4).varCharValue());
        usage.setServiceName(data.get(5).varCharValue());
        usage.setSkuId(data.get(6).varCharValue());
        usage.setSkuDescription(data.get(7).varCharValue());
        usage.setRegion(data.get(8).varCharValue());
        usage.setLocation(data.get(9).varCharValue());
        usage.setCurrency(data.get(10).varCharValue());
        usage.setPricingType(data.get(11).varCharValue());
        usage.setUsageType(data.get(12).varCharValue());

        usage.setUsageAmount(parseBigDecimalSafe(data.get(13).varCharValue()));
        usage.setUsageUnit(data.get(14).varCharValue());
        usage.setUnblendedCost(parseBigDecimalSafe(data.get(15).varCharValue()));
        usage.setBlendedCost(parseBigDecimalSafe(data.get(15).varCharValue()));
        usage.setEffectiveCost(parseBigDecimalSafe(data.get(17).varCharValue()));

        usage.setBillingPeriodStart(
                LocalDateTime.parse(data.get(18).varCharValue().replace(" ", "T"))
        );
        usage.setBillingPeriodEnd(
                LocalDateTime.parse(data.get(19).varCharValue().replace(" ", "T"))
        );

        return usage;
    }

    private BigDecimal parseBigDecimalSafe(String value) {
        return (value == null || value.isEmpty()) ? BigDecimal.ZERO : new BigDecimal(value);
    }

}
