package com.multicloud.batch.service;

import com.multicloud.batch.dto.PerDayCostDTO;
import com.multicloud.batch.enums.CloudProvider;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim Molla
 * Email: shamim.molla@vivasoftltd.com
 */

@Slf4j
@Service
@RequiredArgsConstructor
@Transactional
public class CustomerCostService {

    private final JdbcTemplate jdbcTemplate;

    public List<PerDayCostDTO> findPerDayAzerionCost(long productId,
                                                     long organizationId,
                                                     LocalDate startDate,
                                                     LocalDate endDate) {

        String sql = """
                SELECT usage_date, COALESCE(SUM(net_unblended_cost), 0) AS cost
                FROM aws_billing_daily_costs
                WHERE usage_account_id IN (
                        SELECT account_id FROM product_accounts WHERE product_id = ? AND organization_id = ?
                    )
                    AND usage_date >= ? AND usage_date <= ?
                    AND DATE_FORMAT(usage_date, '%Y-%m') = DATE_FORMAT(billing_month, '%Y-%m')
                    AND (billing_type IN ('Usage', 'SavingsPlanCoveredUsage', 'DiscountedUsage', 'RIFee', 'Fee'))
                GROUP BY 1;
                """;

        RowMapper<PerDayCostDTO> mapper = (rs, rowNum) ->
                new PerDayCostDTO(
                        LocalDate.parse(rs.getString("usage_date")),
                        CloudProvider.AWS,
                        rs.getBigDecimal("cost"),
                        BigDecimal.ZERO,
                        BigDecimal.ZERO,
                        BigDecimal.ZERO
                );

        return jdbcTemplate.query(sql, mapper, productId, organizationId, startDate, endDate);
    }

    public List<PerDayCostDTO> findOutsideOfMonthAzerionCost(long productId,
                                                             long organizationId,
                                                             LocalDate startDate) {

        String sql = """
                SELECT LAST_DAY(billing_month) AS usage_date, COALESCE(SUM(net_unblended_cost), 0) AS cost
                FROM aws_billing_daily_costs
                WHERE usage_account_id IN (
                        SELECT account_id FROM product_accounts WHERE product_id = ? AND organization_id = ?
                    )
                    AND DATE_FORMAT(usage_date, '%Y-%m') <> DATE_FORMAT(billing_month, '%Y-%m')
                    AND (billing_type IN ('Usage', 'SavingsPlanCoveredUsage', 'DiscountedUsage', 'RIFee', 'Fee'))
                    AND billing_month >= ?
                GROUP BY 1;
                """;

        RowMapper<PerDayCostDTO> mapper = (rs, rowNum) ->
                new PerDayCostDTO(
                        LocalDate.parse(rs.getString("usage_date")),
                        CloudProvider.AWS,
                        rs.getBigDecimal("cost"),
                        BigDecimal.ZERO,
                        BigDecimal.ZERO,
                        BigDecimal.ZERO
                );

        return jdbcTemplate.query(sql, mapper, productId, organizationId, startDate);
    }

    public List<PerDayCostDTO> findPerDayCustomerCost(long productId,
                                                      long organizationId,
                                                      boolean isInternalOrg,
                                                      LocalDate startDate,
                                                      LocalDate endDate) {

        String sql = """
                WITH discounts AS (
                    SELECT * FROM daily_organization_pricing
                    WHERE organization_id = ? AND pricing_date >= ? AND pricing_date <= ?
                 ),
                 costs AS (
                    SELECT * FROM service_level_billings slb
                    WHERE usage_account_id IN (
                            SELECT account_id FROM product_accounts WHERE product_id = ? AND organization_id = ?
                        )
                        AND usage_date >= ? AND usage_date <= ?
                 )
                SELECT c.usage_date,
                    c.cloud_provider,
                    SUM(IF(?, COALESCE(c.cost, 0), COALESCE(c.ext_cost, 0))) AS cost,
                    SUM(IF(
                        ?,
                        (COALESCE(c.cost, 0) - (COALESCE(c.cost, 0) * COALESCE(d.discount, 0) / 100)),
                        (COALESCE(c.ext_cost, 0) - (COALESCE(c.ext_cost, 0) * COALESCE(d.discount, 0) / 100))
                    )) AS after_discount_cost,
                    SUM(IF(
                        ?,
                        ((COALESCE(c.cost, 0) - (COALESCE(c.cost, 0) * COALESCE(d.discount, 0) / 100)) * COALESCE(d.handling_fee, 0) / 100),
                        ((COALESCE(c.ext_cost, 0) - (COALESCE(c.ext_cost, 0) * COALESCE(d.discount, 0) / 100)) * COALESCE(d.handling_fee, 0) / 100)
                    )) AS handling_fee,
                    SUM(IF(
                        ?,
                        ((COALESCE(c.cost, 0) - (COALESCE(c.cost, 0) * COALESCE(d.discount, 0) / 100)) * COALESCE(d.support_fee, 0) / 100),
                        ((COALESCE(c.ext_cost, 0) - (COALESCE(c.ext_cost, 0) * COALESCE(d.discount, 0) / 100)) * COALESCE(d.support_fee, 0) / 100)
                    )) AS support_fee
                FROM costs c
                    LEFT JOIN discounts d ON d.cloud_provider = c.cloud_provider AND d.pricing_date = c.usage_date
                GROUP BY 1, 2;
                """;

        RowMapper<PerDayCostDTO> mapper = (rs, rowNum) ->
                new PerDayCostDTO(
                        LocalDate.parse(rs.getString("usage_date")),
                        CloudProvider.valueOf(rs.getString("cloud_provider")),
                        rs.getBigDecimal("cost"),
                        rs.getBigDecimal("after_discount_cost"),
                        rs.getBigDecimal("handling_fee"),
                        rs.getBigDecimal("support_fee")
                );

        return jdbcTemplate.query(
                sql, mapper,
                organizationId, startDate, endDate,
                productId, organizationId, startDate, endDate,
                isInternalOrg, isInternalOrg, isInternalOrg, isInternalOrg
        );
    }

}
