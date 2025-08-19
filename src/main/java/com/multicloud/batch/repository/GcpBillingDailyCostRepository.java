package com.multicloud.batch.repository;

import com.multicloud.batch.model.GcpBillingDailyCost;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim Molla
 * Email: shamim.molla@vivasoftltd.com
 */

@Repository
public interface GcpBillingDailyCostRepository extends JpaRepository<GcpBillingDailyCost, Long> {

    default void upsertGcpBillingDailyCosts(List<GcpBillingDailyCost> bills, JdbcTemplate jdbcTemplate) {

        if (bills == null || bills.isEmpty()) {
            return;
        }

        String sql = """
                    INSERT INTO gcp_billing_daily_costs
                    (organization_id, usage_date, billing_account_id, project_id, project_name,
                     service_code, service_name, sku_id, sku_description, region, location,
                     currency, cost_type, usage_amount, usage_unit, cost, credits)
                    VALUES
                    (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON DUPLICATE KEY UPDATE
                        currency = VALUES(currency),
                        usage_amount = VALUES(usage_amount),
                        usage_unit = VALUES(usage_unit),
                        cost = VALUES(cost),
                        credits = VALUES(credits)
                """;

        jdbcTemplate.batchUpdate(sql, bills, 500, (ps, bill) -> {
            ps.setLong(1, bill.getOrganizationId());
            ps.setDate(2, java.sql.Date.valueOf(bill.getUsageDate()));
            ps.setString(3, bill.getBillingAccountId());
            ps.setString(4, bill.getProjectId());
            ps.setString(5, bill.getProjectName());
            ps.setString(6, bill.getServiceCode());
            ps.setString(7, bill.getServiceName());
            ps.setString(8, bill.getSkuId());
            ps.setString(9, bill.getSkuDescription());
            ps.setString(10, bill.getRegion());
            ps.setString(11, bill.getLocation());
            ps.setString(12, bill.getCurrency());
            ps.setString(13, bill.getCostType());
            ps.setBigDecimal(14, bill.getUsageAmount());
            ps.setString(15, bill.getUsageUnit());
            ps.setBigDecimal(16, bill.getCost());
            ps.setBigDecimal(17, bill.getCredits());
        });

    }

    private static BigDecimal makeRound(BigDecimal value) {
        return value.setScale(8, RoundingMode.HALF_UP);
    }

}
