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
                                                             LocalDate startDate,
                                                             LocalDate endDate) {

        String sql = """
                    SELECT cloud_provider, SUM(final_cost) AS cost
                    FROM service_level_billings
                    WHERE organization_id = ?
                        AND usage_account_id IN (
                            SELECT account_id FROM product_accounts WHERE product_id = ? AND organization_id = ?
                        )
                        AND usage_date >= ?
                        AND usage_date <= ?
                    GROUP BY cloud_provider
                """;

        RowMapper<CloudProviderCostDTO> mapper = (rs, rowNum) ->
                new CloudProviderCostDTO(
                        rs.getString("cloud_provider"),
                        rs.getBigDecimal("cost")
                );

        return jdbcTemplate.query(
                sql, mapper, organizationId, productId, organizationId, startDate, endDate
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
