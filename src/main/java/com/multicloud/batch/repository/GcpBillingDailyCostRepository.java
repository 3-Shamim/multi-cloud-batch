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
                    (usage_date, billing_account_id, project_id, project_name,
                     service_code, service_name, sku_id, sku_description, region, location,
                     currency, cost_type, usage_amount, usage_unit, cost, credits)
                    VALUES
                    (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON DUPLICATE KEY UPDATE
                        project_name = VALUES(project_name),
                        service_name = VALUES(service_name),
                        sku_description = VALUES(sku_description),
                        location = VALUES(location),
                        currency = VALUES(currency),
                        usage_amount = VALUES(usage_amount),
                        usage_unit = VALUES(usage_unit),
                        cost = VALUES(cost),
                        credits = VALUES(credits)
                """;

        jdbcTemplate.batchUpdate(sql, bills, 500, (ps, bill) -> {
            ps.setDate(1, java.sql.Date.valueOf(bill.getUsageDate()));
            ps.setString(2, bill.getBillingAccountId());
            ps.setString(3, bill.getProjectId());
            ps.setString(4, bill.getProjectName());
            ps.setString(5, bill.getServiceCode());
            ps.setString(6, bill.getServiceName());
            ps.setString(7, bill.getSkuId());
            ps.setString(8, bill.getSkuDescription());
            ps.setString(9, bill.getRegion());
            ps.setString(10, bill.getLocation());
            ps.setString(11, bill.getCurrency());
            ps.setString(12, bill.getCostType());
            ps.setBigDecimal(13, bill.getUsageAmount());
            ps.setString(14, bill.getUsageUnit());
            ps.setBigDecimal(15, bill.getCost());
            ps.setBigDecimal(16, bill.getCredits());
        });

    }

    private static BigDecimal makeRound(BigDecimal value) {
        return value.setScale(8, RoundingMode.HALF_UP);
    }

}
