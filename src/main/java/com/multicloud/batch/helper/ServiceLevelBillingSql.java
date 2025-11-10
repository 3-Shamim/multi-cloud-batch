package com.multicloud.batch.helper;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim Molla
 * Email: shamim.molla@vivasoftltd.com
 */

public class ServiceLevelBillingSql {

    public static final String HUAWEI_SQL = """
                SELECT bill_date                                                AS usage_date,
                    'HWC'                                                       AS cloud_provider,
                    payer_account_id                                            AS billing_account_id,
                    customer_id                                                 AS usage_account_id,
                    null                                                        AS usage_account_name,
                    cloud_service_type                                          AS service_code,
                    cloud_service_type_name                                     AS service_name,
                    bill_type                                                   AS billing_type,
                    false                                                       AS is_li_outside_of_month,
                    SUM(consume_amount)                                         AS cost,
                    SUM(ext_official_amount)                                    AS ext_cost
                FROM huawei_billing_daily_costs
                WHERE bill_date >= ? AND bill_date <= ?
                GROUP BY 1, 3, 4, 6, 8;
            """;

    public static final String GCP_SQL = """
                SELECT usage_date,
                    'GCP'                                                       AS cloud_provider,
                    billing_account_id,
                    project_id                                                  AS usage_account_id,
                    project_name                                                AS usage_account_name,
                    service_code,
                    service_name,
                    cost_type                                                   AS billing_type,
                    false                                                       AS is_li_outside_of_month,
                    SUM(COALESCE(cost, 0) + COALESCE(credits, 0))               AS cost,
                    SUM(COALESCE(ext_cost, 0) + COALESCE(ext_credits, 0))       AS ext_cost
                FROM gcp_billing_daily_costs
                WHERE usage_date >= ? AND usage_date <= ?
                    AND DATE_FORMAT(billing_month, '%Y-%m') = DATE_FORMAT(usage_date, '%Y-%m')
                GROUP BY 1, 3, 4, 6, 8;
            """;

    public static final String GCP_EXTRA_LI_SQL = """
                SELECT LAST_DAY(billing_month)                                  AS usage_date,
                    'GCP'                                                       AS cloud_provider,
                    billing_account_id,
                    project_id                                                  AS usage_account_id,
                    project_name                                                AS usage_account_name,
                    service_code,
                    service_name,
                    cost_type                                                   AS billing_type,
                    true                                                        AS is_li_outside_of_month,
                    SUM(COALESCE(cost, 0) + COALESCE(credits, 0))               AS cost,
                    SUM(COALESCE(ext_cost, 0) + COALESCE(ext_credits, 0))       AS ext_cost
                FROM gcp_billing_daily_costs
                WHERE DATE_FORMAT(billing_month, '%Y-%m') <> DATE_FORMAT(usage_date, '%Y-%m')
                    AND billing_month IS NOT NULL
                    AND billing_month >= ?
                GROUP BY 1, 3, 4, 6, 8;
            """;

    public static final String AWS_SQL = """
                SELECT usage_date,
                    'AWS'                                    AS cloud_provider,
                    ''                                       AS billing_account_id,
                    usage_account_id,
                    null                                     AS usage_account_name,
                    service_code,
                    service_name,
                    billing_type,
                    false                                    AS is_li_outside_of_month,
                    SUM(unblended_cost)                      AS cost,
                    SUM(ext_unblended_cost)                  AS ext_cost
                FROM aws_billing_daily_costs
                WHERE usage_date >= ? AND usage_date <= ?
                    AND DATE_FORMAT(billing_month, '%Y-%m') = DATE_FORMAT(usage_date, '%Y-%m')
                GROUP BY 1, 3, 4, 6, 8;
            """;

    public static final String AWS_EXTRA_LI_SQL = """
                SELECT LAST_DAY(billing_month)               AS usage_date,
                    'AWS'                                    AS cloud_provider,
                    ''                                       AS billing_account_id,
                    usage_account_id,
                    null                                     AS usage_account_name,
                    service_code,
                    service_name,
                    billing_type,
                    true                                     AS is_li_outside_of_month,
                    SUM(unblended_cost)                      AS cost,
                    SUM(ext_unblended_cost)                  AS ext_cost
                FROM aws_billing_daily_costs
                WHERE DATE_FORMAT(billing_month, '%Y-%m') <> DATE_FORMAT(usage_date, '%Y-%m')
                    AND billing_month >= ?
                GROUP BY 1, 3, 4, 6, 8;
            """;

    public static final String UPSERT_SQL = """
                INSERT INTO service_level_billings (
                    usage_date,
                    cloud_provider,
                    billing_account_id,
                    usage_account_id,
                    usage_account_name,
                    service_code,
                    service_name,
                    billing_type,
                    is_li_outside_of_month,
                    parent_category,
                    cost,
                    ext_cost
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON DUPLICATE KEY UPDATE
                    usage_account_name = VALUES(usage_account_name),
                    service_name = VALUES(service_name),
                    parent_category = VALUES(parent_category),
                    cost = VALUES(cost),
                    ext_cost = VALUES(ext_cost);
            """;

}
