package com.multicloud.batch.dao.aws;

import com.multicloud.batch.model.AwsBillingDailyCost;
import com.multicloud.batch.repository.AwsBillingDailyCostRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.*;
import java.util.zip.GZIPInputStream;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim Molla
 * Email: shamim.molla@vivasoftltd.com
 */

@Slf4j
@RequiredArgsConstructor
@Service
public class AwsBillingServiceImpl implements AwsBillingService {

    private final static String[] COLS = {
            "usage_date", "billing_month", "usage_account_id", "service_code", "service_name",
            "sku_id", "region", "location", "currency", "pricing_type", "billing_type",
            "usage_type", "usage_amount", "usage_unit", "unblended_cost", "blended_cost", "net_unblended_cost"
    };

    private final JdbcTemplate jdbcTemplate;
    private final AthenaService athenaService;
    private final AwsBillingDailyCostRepository awsBillingDailyCostRepository;

    @Override
    public Set<String> tableListByDatabase(String database, String accessKey, String secretKey, String region) {

        StaticCredentialsProvider credentialsProvider = StaticCredentialsProvider.create(
                AwsBasicCredentials.create(accessKey, secretKey)
        );

        AthenaClient athenaClient = AthenaClient.builder()
                .credentialsProvider(credentialsProvider)
                .region(Region.of(region))
                .build();

        String query = "SHOW TABLES";

        String bucket = "azerion-athena-results";
        String prefix = "azerion_mc";

        String outputLocation = "s3://%s/%s/".formatted(bucket, prefix);

        String executionId = athenaService.submitAthenaQuery(query, outputLocation, database, athenaClient);
        athenaService.waitForQueryToComplete(executionId, athenaClient);

        Set<String> tables = new HashSet<>();

        athenaService.fetchQueryResults(executionId, athenaClient)
                .forEach(res -> res.resultSet().rows()
                        .forEach(row -> tables.add(row.data().getFirst().varCharValue())));

        return tables;
    }

    @Override
    public void syncDailyCostUsageFromAthena(String database, String tableName,
                                             String accessKey, String secretKey, String region,
                                             LocalDate start, LocalDate end, boolean internal) {

        int year = start.getYear();
        int month = start.getMonthValue();

        LocalDate startOfNextMonth = end.plusMonths(1).withDayOfMonth(1);

        String netUnblendedCol = "CAST(0 AS DECIMAL(20, 8)) AS net_unblended_cost";

        if (internal) {
            netUnblendedCol = """
                        CAST(COALESCE(SUM(line_item_net_unblended_cost), 0) AS DECIMAL(20, 8))   AS net_unblended_cost
                    """;
        }

        String query = """
                SELECT DATE(line_item_usage_start_date)                                      AS usage_date,
                
                    DATE(CONCAT(year, '-', month, '-01'))                                    AS billing_month,
                
                    -- Linked/Usage account ID
                    line_item_usage_account_id                                               AS usage_account_id,
                
                    -- Service
                    COALESCE(product_servicecode, '')                                        AS service_code,
                    CONCAT('"', COALESCE(MAX(product_servicename), ''), '"')                 AS service_name,
                
                    -- SKU
                    COALESCE(product_sku, '')                                                AS sku_id,
                
                    -- Region & Location
                    COALESCE(product_region, '')                                             AS region,
                    CONCAT('"', COALESCE(MAX(product_location), ''), '"')                    AS location,
                
                    -- Currency & Usage & Cost
                    COALESCE(MAX(line_item_currency_code), '')                               AS currency,
                    COALESCE(MAX(pricing_term), 'OnDemand')                                  AS pricing_type,
                    COALESCE(line_item_line_item_type, '')                                   AS billing_type,
                    COALESCE(line_item_usage_type, '')                                       AS usage_type,
                
                    CAST(COALESCE(SUM(line_item_usage_amount), 0) AS DECIMAL(20, 8))         AS usage_amount,
                    MAX(pricing_unit)                                                        AS usage_unit,
                
                    CAST(COALESCE(SUM(line_item_unblended_cost), 0) AS DECIMAL(20, 8))       AS unblended_cost,
                    CAST(COALESCE(SUM(line_item_blended_cost), 0) AS DECIMAL(20, 8))         AS blended_cost,
                    %s
                
                FROM "%s"
                WHERE CAST(year AS INTEGER) = %d
                    AND CAST(month AS INTEGER) = %d
                    AND (
                        DATE(line_item_usage_start_date) >= DATE '%s' AND DATE(line_item_usage_start_date) <= DATE '%s'
                        OR DATE(line_item_usage_start_date) >= DATE '%s'
                    )
                GROUP BY 1, 2, 3, 4, 6, 7, 11, 12
                """.formatted(netUnblendedCol, tableName, year, month, start, end, startOfNextMonth);

        syncDailyCostUsageFromAthena(database, query, accessKey, secretKey, region, internal);

    }

    @Override
    public void syncDailyCostUsageFromAthenaV2(String database, String tableName,
                                               String accessKey, String secretKey, String region,
                                               LocalDate start, LocalDate end, boolean internal) {

        int year = start.getYear();
        int month = start.getMonthValue();

        LocalDate startOfNextMonth = end.plusMonths(1).withDayOfMonth(1);

        String netUnblendedCol = "CAST(0 AS DECIMAL(20, 8)) AS net_unblended_cost";

        if (internal) {
            netUnblendedCol = """
                        CAST(COALESCE(SUM(line_item_net_unblended_cost), 0) AS DECIMAL(20, 8))   AS net_unblended_cost
                    """;
        }

        String query = """
                SELECT DATE(line_item_usage_start_date)                                      AS usage_date,
                
                    DATE(CONCAT(billing_period, '-01'))                                      AS billing_month,
                
                    -- Linked/Usage account ID
                    line_item_usage_account_id                                               AS usage_account_id,
                
                    -- Service
                    COALESCE(product_servicecode, '')                                        AS service_code,
                    CONCAT('"', COALESCE(MAX(product['servicename']), ''), '"')              AS service_name,
                
                    -- SKU
                    COALESCE(product_sku, '')                                                AS sku_id,
                
                    -- Region & Location
                    COALESCE(product_region_code, '')                                        AS region,
                    CONCAT('"', COALESCE(MAX(product_location), ''), '"')                    AS location,
                
                    -- Currency & Usage & Cost
                    COALESCE(MAX(line_item_currency_code), '')                               AS currency,
                    COALESCE(MAX(pricing_term), 'OnDemand')                                  AS pricing_type,
                    COALESCE(line_item_line_item_type, '')                                   AS billing_type,
                    COALESCE(line_item_usage_type, '')                                       AS usage_type,
                
                    CAST(COALESCE(SUM(line_item_usage_amount), 0) AS DECIMAL(20, 8))         AS usage_amount,
                    MAX(pricing_unit)                                                        AS usage_unit,
                
                    CAST(COALESCE(SUM(line_item_unblended_cost), 0) AS DECIMAL(20, 8))       AS unblended_cost,
                    CAST(COALESCE(SUM(line_item_blended_cost), 0) AS DECIMAL(20, 8))         AS blended_cost,
                    %s
                
                FROM "%s"
                WHERE billing_period = '%d-%02d'
                    AND (
                        DATE(line_item_usage_start_date) >= DATE '%s' AND DATE(line_item_usage_start_date) <= DATE '%s'
                        OR DATE(line_item_usage_start_date) >= DATE '%s'
                    )
                GROUP BY 1, 2, 3, 4, 6, 7, 11, 12
                """.formatted(netUnblendedCol, tableName, year, month, start, end, startOfNextMonth);

        syncDailyCostUsageFromAthena(database, query, accessKey, secretKey, region, internal);

    }

    // Table name represents view name
    @Override
    public void syncDailyCostUsageFromAthenaView(String database, String tableName,
                                                 String accessKey, String secretKey, String region,
                                                 LocalDate start, LocalDate end) {

        int year = start.getYear();
        int month = start.getMonthValue();

        LocalDate startOfNextMonth = end.plusMonths(1).withDayOfMonth(1);

        String query = """
                SELECT
                    usage_date,
                    billing_month,
                    usage_account_id,
                    service_code,
                    service_name,
                    sku_id,
                    region,
                    location,
                    currency,
                    pricing_type,
                    billing_type,
                    usage_type,
                    usage_amount,
                    usage_unit,
                    CAST(cost AS DECIMAL(20, 8)) AS unblended_cost,
                    CAST(0 AS DECIMAL(20, 8))    AS blended_cost,
                    CAST(0 AS DECIMAL(20, 8))    AS net_unblended_cost
                FROM "%s"
                WHERE CAST(year AS INTEGER) = %d
                    AND CAST(month AS INTEGER) = %d
                    AND (
                        (usage_date >= DATE '%s' AND usage_date <= DATE '%s')
                        OR (
                            CAST(year AS INTEGER) = %d AND CAST(month AS INTEGER) = %d
                            AND usage_date >= DATE '%s'
                        )
                    )
                """.formatted(tableName, year, month, start, end, year, month, startOfNextMonth);

        syncDailyCostUsageFromAthena(database, query, accessKey, secretKey, region, false);

    }

    private void syncDailyCostUsageFromAthena(String database, String query,
                                              String accessKey, String secretKey, String region,
                                              boolean internal) {

        StaticCredentialsProvider credentialsProvider = StaticCredentialsProvider.create(
                AwsBasicCredentials.create(accessKey, secretKey)
        );

        AthenaClient athenaClient = AthenaClient.builder()
                .credentialsProvider(credentialsProvider)
                .region(Region.of(region))
                .build();

        S3Client s3Client = S3Client.builder()
                .credentialsProvider(credentialsProvider)
                .region(Region.of(region))
                .build();

        String bucket = "azerion-athena-results";
        String prefix = "azerion_mc";

        // If you use 'unload' -- it required empty folder on S3,
        // It may generate multiple files
        prefix = "%s/%s/%s".formatted(prefix, "unloaded_data", UUID.randomUUID().toString());

        String outputLocation = "s3://%s/%s/".formatted(bucket, prefix);

        // Wrap query for unload
        query = athenaService.wrapQueryWithUnloadCsvGzip(query, outputLocation);

        String executionId = athenaService.submitAthenaQuery(query, outputLocation, database, athenaClient);
        athenaService.waitForQueryToComplete(executionId, athenaClient);

        long totalResults = fetchResultsToUnloadedFileFromS3(bucket, prefix, s3Client, internal);

        log.info("AWS billing data fetched and stored successfully. Total results: {}", totalResults);

    }

    private long fetchResultsToUnloadedFileFromS3(String bucket, String prefix, S3Client s3Client, boolean internal) {

        ListObjectsV2Request s3Request = ListObjectsV2Request.builder()
                .bucket(bucket)
                .prefix(prefix)
                .build();

        ListObjectsV2Response s3Response = s3Client.listObjectsV2(s3Request);

        List<AwsBillingDailyCost> results = new ArrayList<>();
        long count = 0;

        for (S3Object s3Object : s3Response.contents()) {

            String key = s3Object.key();

            if (key.endsWith(".gz")) {

                log.info("Processing AWS Athena unloaded file: {}", key);

                // Read and parse this file only
                GetObjectRequest getRequest = GetObjectRequest.builder()
                        .bucket(bucket)
                        .key(key)
                        .build();

                try (
                        ResponseInputStream<GetObjectResponse> s3Stream = s3Client.getObject(getRequest);
                        BufferedReader reader = new BufferedReader(
                                new InputStreamReader(new GZIPInputStream(s3Stream))
                        )
                ) {

                    Iterable<CSVRecord> records = CSVFormat.DEFAULT
                            .builder()
                            .setHeader(COLS)
                            .get()
                            .parse(reader);

                    for (CSVRecord record : records) {

                        results.add(bindRecord(record, internal));
                        count++;

                        if (results.size() == 5000) {
                            // Save each batch
                            // Clean before the next
                            log.info("Upserting {} fetched AWS records into DB.", results.size());
                            awsBillingDailyCostRepository.upsertAwsBillingDailyCosts(results, jdbcTemplate, internal);
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
        awsBillingDailyCostRepository.upsertAwsBillingDailyCosts(results, jdbcTemplate, internal);
        results.clear();

        return count;
    }

    private AwsBillingDailyCost bindRecord(CSVRecord record, boolean internal) {

        return AwsBillingDailyCost.builder()
                .usageDate(LocalDate.parse(record.get("usage_date")))
                .billingMonth(LocalDate.parse(record.get("billing_month")))
                .usageAccountId(record.get("usage_account_id"))
                .serviceCode(record.get("service_code"))
                .serviceName(record.get("service_name"))
                .skuId(record.get("sku_id"))
                .region(record.get("region"))
                .location(record.get("location"))
                .currency(record.get("currency"))
                .pricingType(record.get("pricing_type"))
                .billingType(record.get("billing_type"))
                .usageType(record.get("usage_type"))
                .usageAmount(parseBigDecimalSafe(record.get("usage_amount")))
                .usageUnit(record.get("usage_unit"))
                .unblendedCost(internal ? parseBigDecimalSafe(record.get("unblended_cost")) : BigDecimal.ZERO)
                .blendedCost(internal ? parseBigDecimalSafe(record.get("blended_cost")) : BigDecimal.ZERO)
                .netUnblendedCost(internal ? parseBigDecimalSafe(record.get("net_unblended_cost")) : BigDecimal.ZERO)
                .extUnblendedCost(internal ? BigDecimal.ZERO : parseBigDecimalSafe(record.get("unblended_cost")))
                .extBlendedCost(internal ? BigDecimal.ZERO : parseBigDecimalSafe(record.get("blended_cost")))
                .extNetUnblendedCost(internal ? BigDecimal.ZERO : parseBigDecimalSafe(record.get("net_unblended_cost")))
                .build();
    }

    private BigDecimal parseBigDecimalSafe(String value) {
        try {
            return (value == null || value.isEmpty()) ? BigDecimal.ZERO : new BigDecimal(value);
        } catch (Exception e) {
            log.error("Error parsing BigDecimal: {}", value, e);
            throw new RuntimeException(e.getMessage());
        }
    }

}
