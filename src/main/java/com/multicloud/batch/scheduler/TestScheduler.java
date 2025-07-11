package com.multicloud.batch.scheduler;

import lombok.RequiredArgsConstructor;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.athena.model.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim Molla
 * Email: shamim.molla@vivasoftltd.com
 */

@RequiredArgsConstructor
@Component
public class TestScheduler {

    private final AthenaClient athenaClient;

//    @Scheduled(fixedDelay = 1, timeUnit = TimeUnit.DAYS)
    public void test() throws InterruptedException {

        System.out.println("===========================================");
        System.out.println("===========================================");

//        String query = "desc athena";
//        String query = "show partitions athena";
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
                            line_item_usage_start_date >= DATE_ADD('day', -7, CURRENT_DATE)
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
                    """;

        String executionId = submitAthenaQuery(query);
        waitForQueryToComplete(executionId);
        List<Map<String, String>> results = getQueryResults(executionId);

        results.forEach(System.out::println);

        System.out.println("===========================================");
        System.out.println("===========================================");

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
