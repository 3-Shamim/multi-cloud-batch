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
                    COALESCE(consume_amount, 0) - COALESCE(coupon_amount, 0)    AS cost
                FROM huawei_billing_daily_costs
                WHERE bill_date >= ? AND bill_date <= ?
                GROUP BY 1, 2, 3, 4, 6, 8;
            """;

    public static final String GCP_SQL = """
                SELECT usage_date,
                    'GCP'                                   AS cloud_provider,
                    billing_account_id,
                    project_id                              AS usage_account_id,
                    project_name                            AS usage_account_name,
                    service_code,
                    service_name,
                    cost_type                               AS billing_type,
                    cost
                FROM gcp_billing_daily_costs
                WHERE usage_date >= ? AND usage_date <= ?
                GROUP BY 1, 2, 3, 4, 6, 8;
            """;

    public static final String AWS_SQL = """
                SELECT usage_date,
                       'AWS'                                    AS cloud_provider,
                       payer_account_id                         AS billing_account_id,
                       usage_account_id,
                       null                                     AS usage_account_name,
                       service_code,
                       service_name,
                       billing_type,
                       unblended_cost                           AS cost
                FROM aws_billing_daily_costs
                WHERE usage_date >= ? AND usage_date <= ?
                GROUP BY 1, 2, 3, 4, 6, 8;
            """;

    public static final String UPSERT_SQL = """
                INSERT INTO service_level_billings (
                    cloud_provider,
                    usage_date,
                    billing_account_id,
                    usage_account_id,
                    usage_account_name,
                    service_code,
                    service_name,
                    billing_type,
                    parent_category,
                    cost
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON DUPLICATE KEY UPDATE
                    usage_account_name = VALUES(usage_account_name),
                    service_name = VALUES(service_name),
                    parent_category = VALUES(parent_category),
                    cost = VALUES(cost);
            """;

}
