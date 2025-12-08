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
@Transactional
public class InvoiceCostService {

    private final JdbcTemplate jdbcTemplate;

    public List<CloudProviderCostDTO> findCloudProviderCosts(long productId,
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
                    SELECT c.cloud_provider,
                            SUM(IF(
                                ?,
                                COALESCE(c.cost, 0) - (COALESCE(c.cost, 0) * COALESCE(d.discount, 0) / 100),
                                COALESCE(c.ext_cost, 0) - (COALESCE(c.ext_cost, 0) * COALESCE(d.discount, 0) / 100)
                            )) AS cost
                    FROM costs c
                        LEFT JOIN discounts d ON d.cloud_provider = c.cloud_provider AND d.pricing_date = c.usage_date
                    GROUP BY 1;
                """;

        RowMapper<CloudProviderCostDTO> mapper = (rs, rowNum) ->
                new CloudProviderCostDTO(
                        rs.getString("cloud_provider"),
                        rs.getBigDecimal("cost")
                );

        return jdbcTemplate.query(
                sql, mapper, organizationId, startDate, endDate, productId, organizationId, startDate, endDate, isInternalOrg
        );
    }

    public long getLatestInvoiceNumber(long productId) {

        String sql = """
                    SELECT invoice_number FROM billings WHERE product_id = ? ORDER BY invoice_number DESC LIMIT 1;
                """;

        List<Long> invoiceNumberList = jdbcTemplate.queryForList(
                sql, Long.class, productId
        );

        return invoiceNumberList.isEmpty() ? 0 : invoiceNumberList.getFirst();
    }

}
