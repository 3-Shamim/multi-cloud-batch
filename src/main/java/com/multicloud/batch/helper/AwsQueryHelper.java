package com.multicloud.batch.helper;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim Molla
 * Email: shamim.molla@vivasoftltd.com
 */

public class AwsQueryHelper {

    public final static String GENERAL_AMORTIZE_COST_QUERY = """
            SELECT DATE(line_item_usage_start_date) AS usage_date,
                o.billing_entity AS billing_entity,
                SUM(
                    (
                        (
                            (
                                COALESCE(a.reservation_net_effective_cost, 0) + COALESCE(
                                    a.savings_plan_net_savings_plan_effective_cost,
                                    0
                                )
                            ) + COALESCE(
                                (
                                    CASE
                                        WHEN (
                                            (a.line_item_line_item_type IN ('Fee', 'RIFee'))
                                            AND (a.bill_billing_entity = 'AWS')
                                        ) THEN 0
                                        WHEN (
                                            (a.line_item_line_item_type = 'Fee')
                                            AND (a.bill_billing_entity = 'AWS Marketplace')
                                            AND (a.line_item_usage_account_id = '215378263321')
                                        ) THEN 0 ELSE a.line_item_net_unblended_cost
                                    END
                                ),
                                0
                            )
                        ) + COALESCE(a.reservation_net_unused_recurring_fee, 0)
                    )
                ) AS cost
            FROM athena a
                LEFT JOIN org_accounts o ON (a.line_item_usage_account_id = o.account_id)
            WHERE a.line_item_line_item_type IN (
                    'Usage', 'SavingsPlanCoveredUsage', 'DiscountedUsage', 'RIFee', 'Fee'
                )
                AND o.billing_entity IN ('hitta', 'woozworld', 'gembly-bv', 'whow-games-gmbh', 'adinmo')
                AND CAST(year AS INTEGER) = %d AND CAST(month AS INTEGER) = %d
            GROUP BY 1, 2
            HAVING (
                SUM(
                    (
                        (
                            (
                                COALESCE(a.reservation_net_effective_cost, 0) + COALESCE(
                                    a.savings_plan_net_savings_plan_effective_cost,
                                    0
                                )
                            ) + COALESCE(
                                (
                                    CASE
                                        WHEN (
                                            (a.line_item_line_item_type IN ('Fee', 'RIFee'))
                                            AND (a.bill_billing_entity = 'AWS')
                                        ) THEN 0
                                        WHEN (
                                            (a.line_item_line_item_type = 'Fee')
                                            AND (a.bill_billing_entity = 'AWS Marketplace')
                                            AND (a.line_item_usage_account_id = '215378263321')
                                        ) THEN 0 ELSE a.line_item_net_unblended_cost
                                    END
                                ),
                                0
                            )
                        ) + COALESCE(a.reservation_net_unused_recurring_fee, 0)
                    )
                ) > 0
            )
            """;

    public final static String HKTS_AMORTIZE_COST_QUERY = """
            SELECT DATE(line_item_usage_start_date) AS usage_date,
                'hkts' as billing_entity,
                SUM(
                    (
                        (
                            (
                                COALESCE(a.reservation_net_effective_cost, 0) + COALESCE(
                                    a.savings_plan_net_savings_plan_effective_cost,
                                    0
                                )
                            ) + COALESCE(
                                (
                                    CASE
                                        WHEN (
                                            (a.line_item_line_item_type IN ('Fee', 'RIFee'))
                                            AND (a.bill_billing_entity = 'AWS')
                                        ) THEN 0
                                        WHEN (
                                            (a.line_item_line_item_type = 'Fee')
                                            AND (a.bill_billing_entity = 'AWS Marketplace')
                                            AND (a.line_item_usage_account_id = '215378263321')
                                        ) THEN 0 ELSE a.line_item_net_unblended_cost
                                    END
                                ),
                                0
                            )
                        ) + COALESCE(a.reservation_net_unused_recurring_fee, 0)
                    )
                ) AS cost
            FROM athena a
            WHERE  a.line_item_line_item_type IN (
                    'Usage', 'SavingsPlanCoveredUsage', 'DiscountedUsage', 'RIFee', 'Fee'
                )
                AND a.resource_tags_user_client_env = 'hkts'
                AND a.line_item_usage_account_id = '312563481632'
                AND CAST(year AS INTEGER) = %d AND CAST(month AS INTEGER) = %d
            GROUP BY 1, 2
            HAVING (
                SUM(
                    (
                        (
                            (
                                COALESCE(a.reservation_net_effective_cost, 0) + COALESCE(
                                    a.savings_plan_net_savings_plan_effective_cost,
                                    0
                                )
                            ) + COALESCE(
                                (
                                    CASE
                                        WHEN (
                                            (a.line_item_line_item_type IN ('Fee', 'RIFee'))
                                            AND (a.bill_billing_entity = 'AWS')
                                        ) THEN 0
                                        WHEN (
                                            (a.line_item_line_item_type = 'Fee')
                                            AND (a.bill_billing_entity = 'AWS Marketplace')
                                            AND (a.line_item_usage_account_id = '215378263321')
                                        ) THEN 0 ELSE a.line_item_net_unblended_cost
                                    END
                                ),
                                0
                            )
                        ) + COALESCE(a.reservation_net_unused_recurring_fee, 0)
                    )
                ) > 0
            )
            """;

}
