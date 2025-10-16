package com.multicloud.batch.service;

import com.multicloud.batch.dto.BillingDTO;
import com.multicloud.batch.dto.CloudProviderCostDTO;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Date;
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
@Transactional(readOnly = true)
public class InvoiceCostService {

    private final JdbcTemplate jdbcTemplate;

    public List<CloudProviderCostDTO> findCloudProviderCosts(long productId,
                                                             long organizationId,
                                                             boolean internalOrg,
                                                             LocalDate startDate,
                                                             LocalDate endDate) {

        String sql = """
                    WITH discounts AS (
                        SELECT * FROM daily_organization_pricing WHERE organization_id = ?
                     ),
                     costs AS (
                        SELECT * FROM service_level_billings slb
                        WHERE usage_account_id IN (
                                SELECT account_id FROM product_accounts WHERE product_id = ? AND organization_id = ?
                            )
                            AND usage_date >= ? AND usage_date <= ?
                     )
                    SELECT c.cloud_provider,
                            SUM(IF(
                                    ?,
                                    COALESCE(cost, 0) - (COALESCE(cost, 0) * COALESCE(d.discount, 0) / 100),
                                    COALESCE(ext_cost, 0) - (COALESCE(ext_cost, 0) * COALESCE(d.discount, 0) / 100)
                               )
                            ) AS cost
                    FROM costs c
                        LEFT JOIN discounts d ON d.cloud_provider = c.cloud_provider AND d.date = c.usage_date
                    GROUP BY 1;
                """;

        RowMapper<CloudProviderCostDTO> mapper = (rs, rowNum) ->
                new CloudProviderCostDTO(
                        rs.getString("cloud_provider"),
                        rs.getBigDecimal("cost")
                );

        return jdbcTemplate.query(
                sql, mapper, organizationId, productId, organizationId, startDate, endDate, internalOrg
        );
    }

    public void insert(List<BillingDTO> billings) {

        String query = """
                INSERT INTO billings(month_date, product_id, organization_id, cloud_provider, cost)
                VALUES (?, ?, ?, ?, ?);
                """;

        jdbcTemplate.batchUpdate(query, billings, 100, (ps, bill) -> {
            ps.setDate(1, Date.valueOf(bill.month().atDay(1)));
            ps.setLong(2, bill.productId());
            ps.setLong(3, bill.organizationId());
            ps.setString(4, bill.provider().name());
            ps.setBigDecimal(5, bill.cost());
        });

    }

}
