package com.multicloud.batch.service;

import com.multicloud.batch.dto.PerDayCostDTO;
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
public class AwsCustomerCostService {

    private final JdbcTemplate jdbcTemplate;

    public List<PerDayCostDTO> findPerDayAzerionCost(long productId,
                                                     long organizationId,
                                                     LocalDate startDate,
                                                     LocalDate endDate) {

        String sql = """
                SELECT usage_date, SUM(net_unblended_cost) AS cost
                FROM aws_billing_daily_costs
                WHERE usage_account_id IN (
                        SELECT account_id FROM product_accounts WHERE product_id = ? AND organization_id = ?
                    )
                    AND usage_date >= ? AND usage_date <= ?
                    AND (billing_type IN ('Usage', 'SavingsPlanCoveredUsage', 'DiscountedUsage', 'RIFee', 'Fee'))
                GROUP BY 1;
                """;

        RowMapper<PerDayCostDTO> mapper = (rs, rowNum) ->
                new PerDayCostDTO(
                        LocalDate.parse(rs.getString("usage_date")),
                        rs.getBigDecimal("cost"),
                        BigDecimal.ZERO,
                        BigDecimal.ZERO
                );

        return jdbcTemplate.query(
                sql, mapper, productId, organizationId, startDate, endDate
        );

    }

    public List<PerDayCostDTO> findPerDayCustomerCost(long productId,
                                                      long organizationId,
                                                      boolean isInternalOrg,
                                                      LocalDate startDate,
                                                      LocalDate endDate) {

        String sql = """
                WITH discounts AS (
                    SELECT * FROM daily_organization_pricing
                    WHERE organization_id = ? AND cloud_provider = 'AWS'
                        AND pricing_date >= ? AND pricing_date <= ?
                 ),
                 costs AS (
                    SELECT * FROM service_level_billings slb
                    WHERE usage_account_id IN (
                            SELECT account_id FROM product_accounts WHERE product_id = ? AND organization_id = ?
                        )
                        AND cloud_provider = 'AWS' AND usage_date >= ? AND usage_date <= ?
                 )
                SELECT c.usage_date,
                    SUM(IF(
                        ?,
                        (COALESCE(c.cost, 0) - (COALESCE(c.cost, 0) * COALESCE(d.discount, 0) / 100)),
                        (COALESCE(c.ext_cost, 0) - (COALESCE(c.ext_cost, 0) * COALESCE(d.discount, 0) / 100))
                    )) AS cost,
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
                GROUP BY 1;
                """;

        RowMapper<PerDayCostDTO> mapper = (rs, rowNum) ->
                new PerDayCostDTO(
                        LocalDate.parse(rs.getString("usage_date")),
                        rs.getBigDecimal("cost"),
                        rs.getBigDecimal("handling_fee"),
                        rs.getBigDecimal("support_fee")
                );

        return jdbcTemplate.query(
                sql, mapper,
                organizationId, startDate, endDate,
                productId, organizationId, startDate, endDate,
                isInternalOrg, isInternalOrg, isInternalOrg
        );
    }

}
