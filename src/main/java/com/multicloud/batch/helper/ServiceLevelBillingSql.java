package com.multicloud.batch.helper;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim Molla
 * Email: shamim.molla@vivasoftltd.com
 */

public class ServiceLevelBillingSql {

    public static final String HUAWEI_SQL = """
                SELECT bill_date               AS usage_date,
                       organization_id,
                       'HWC'                   AS cloud_provider,
                       payer_account_id        AS billing_account_id,
                       customer_id             AS usage_account_id,
                       customer_id             AS usage_account_name,
                       cloud_service_type      AS service_code,
                       cloud_service_type_name AS service_name,
                       SUM(consume_amount)     AS cost
                FROM huawei_billing_daily_costs
                GROUP BY 1, 2, 3, 4, 6;
            """;

    public static final String GCP_SQL = """
                SELECT usage_date,
                       organization_id,
                       'GCP'        AS cloud_provider,
                       billing_account_id,
                       project_id   AS usage_account_id,
                       project_name AS usage_account_name,
                       service_code,
                       service_name,
                       SUM(cost)    AS cost
                FROM gcp_billing_daily_costs
                GROUP BY 1, 2, 3, 4, 6;
            """;


    public static final String AWS_SQL = """
                SELECT usage_date,
                       organization_id,
                       'AWS'               AS cloud_provider,
                       payer_account_id    AS billing_account_id,
                       usage_account_id,
                       usage_account_id    AS usage_account_name,
                       service_code,
                       service_name,
                       SUM(unblended_cost) AS cost
                FROM aws_billing_daily_costs
                GROUP BY 1, 2, 3, 4, 6;
            """;

    public static final String UPSERT_SQL = """
                INSERT INTO service_level_billings (
                    organization_id,
                    cloud_provider,
                    usage_date,
                    billing_account_id,
                    usage_account_id,
                    usage_account_name,
                    service_code,
                    service_name,
                    cost
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON DUPLICATE KEY UPDATE
                    usage_account_name = VALUES(usage_account_name),
                    service_name = VALUES(service_name),
                    cost = VALUES(cost);
            """;

}
