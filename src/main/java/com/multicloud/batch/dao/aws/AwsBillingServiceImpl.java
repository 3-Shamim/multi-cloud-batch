package com.multicloud.batch.dao.aws;

import com.multicloud.batch.cloud_config.aws.AwsDynamicCredentialsProvider;
import com.multicloud.batch.enums.CloudProvider;
import com.multicloud.batch.enums.LastSyncStatus;
import com.multicloud.batch.model.CloudDailyBilling;
import com.multicloud.batch.repository.CloudDailyBillingRepository;
import jakarta.persistence.EntityManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.util.Pair;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.athena.model.*;
import software.amazon.awssdk.services.costexplorer.CostExplorerClient;
import software.amazon.awssdk.services.costexplorer.model.*;

import java.math.BigDecimal;
import java.time.LocalDate;
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
    public Pair<LastSyncStatus, String> syncDailyCostUsageFromAthena(long organizationId, String accessKey, String secretKey, boolean firstSync) {

        AwsBasicCredentials credentials = AwsBasicCredentials.create(accessKey, secretKey);
        AwsDynamicCredentialsProvider.setAwsCredentials(credentials);

        try {

            int days;

            if (firstSync) {
                days = 365;
            } else {
                days = 7;
            }

            String query = """
                        SELECT
                            DATE(line_item_usage_start_date) AS usage_date,
    
                            -- Account
                            bill_payer_account_id AS payer_account_id,
                            line_item_usage_account_id AS usage_account_id,

                            -- Project (from tags)
                            COALESCE(resource_tags_user_project, 'unassigned') AS project,
                            resource_tags_user_env AS environment,

                            -- Region / Location
                            product_region AS region,
                            product_location AS location,

                            -- Service & SKU
                            product_servicename AS service_name,
                            product_sku,

                            -- SKU readable label (fallback chain)
                            COALESCE(
                            NULLIF(line_item_line_item_description, ''),
                            NULLIF(product_product_name, ''),
                            'Unknown SKU'
                            ) AS sku_label,

                            -- Usage type
                            line_item_usage_type,

                            -- Usage and Cost
                            SUM(line_item_usage_amount) AS usage_amount,
                            pricing_unit AS usage_unit,
                            SUM(line_item_unblended_cost) AS unblended_cost,

                            -- Currency & Pricing
                            line_item_currency_code AS currency,
                            pricing_term,
                            pricing_purchase_option,
                            pricing_offering_class

                        FROM athena

                        WHERE
                            line_item_usage_start_date >= DATE_ADD('day', -%d, CURRENT_DATE)
                            AND line_item_line_item_type = 'Usage' -- filter out credits, taxes, etc.

                        GROUP BY
                            DATE(line_item_usage_start_date),
                            bill_payer_account_id,
                            line_item_usage_account_id,
                            resource_tags_user_project,
                            resource_tags_user_env,
                            product_region,
                            product_location,
                            product_servicename,
                            product_sku,
                            line_item_line_item_description,
                            product_product_name,
                            line_item_usage_type,
                            pricing_unit,
                            line_item_currency_code,
                            pricing_term,
                            pricing_purchase_option,
                            pricing_offering_class
                        ORDER BY usage_date DESC;
                    """.formatted(days);

            String executionId = submitAthenaQuery(query);
            waitForQueryToComplete(executionId);
            List<Map<String, String>> results = getQueryResults(executionId);

            results.forEach(System.out::println);


            List<CloudDailyBilling> billings = new ArrayList<>();


//            cloudDailyBillingRepository.batchUpsert(billings, entityManager);

            return Pair.of(LastSyncStatus.SUCCESS, "Successfully synced [%d] items.".formatted(billings.size()));

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

        return response.queryExecutionId();
    }

    public void waitForQueryToComplete(String executionId) throws InterruptedException {

        while (true) {

            GetQueryExecutionRequest getRequest = GetQueryExecutionRequest.builder()
                    .queryExecutionId(executionId)
                    .build();

            QueryExecutionState state = athenaClient.getQueryExecution(getRequest)
                    .queryExecution().status().state();

            if (state == QueryExecutionState.SUCCEEDED) return;

            if (state == QueryExecutionState.FAILED || state == QueryExecutionState.CANCELLED) {
                throw new RuntimeException("Query failed or was cancelled");
            }

            Thread.sleep(1000); // Poll every second
        }

    }

    public List<Map<String, String>> getQueryResults(String executionId) {

        List<Map<String, String>> results = new ArrayList<>();

        GetQueryResultsRequest resultRequest = GetQueryResultsRequest.builder()
                .queryExecutionId(executionId)
                .build();

        GetQueryResultsResponse resultResponse = athenaClient.getQueryResults(resultRequest);

        List<ColumnInfo> columnInfoList = resultResponse.resultSet().resultSetMetadata().columnInfo();

        List<Row> rows = resultResponse.resultSet().rows();

        for (int i = 1; i < rows.size(); i++) { // skip header

            Row row = rows.get(i);

            Map<String, String> rowMap = new HashMap<>();

            for (int j = 0; j < row.data().size(); j++) {
                rowMap.put(columnInfoList.get(j).name(), row.data().get(j).varCharValue());
            }

            results.add(rowMap);

        }

        return results;
    }

}
