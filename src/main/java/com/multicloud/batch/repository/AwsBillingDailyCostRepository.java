package com.multicloud.batch.repository;

import com.multicloud.batch.model.AwsBillingDailyCost;
import jakarta.persistence.EntityManager;
import jakarta.persistence.Query;
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
public interface AwsBillingDailyCostRepository extends JpaRepository<AwsBillingDailyCost, Long> {

    default void upsertAwsBillingDailyCosts(List<AwsBillingDailyCost> bills, JdbcTemplate jdbcTemplate) {

        String sql = """
                    INSERT INTO aws_billing_daily_costs
                    (organization_id, usage_date, payer_account_id, usage_account_id,
                     service_code, service_name, sku_id, sku_description, region, location,
                     currency, pricing_type, billing_type, usage_type, usage_amount, usage_unit,
                     unblended_cost, blended_cost, net_cost)
                    VALUES
                    (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON DUPLICATE KEY UPDATE
                        currency = VALUES(currency),
                        pricing_type = VALUES(pricing_type),
                        usage_amount = VALUES(usage_amount),
                        usage_unit = VALUES(usage_unit),
                        unblended_cost = VALUES(unblended_cost),
                        blended_cost = VALUES(blended_cost),
                        net_cost = VALUES(net_cost)
                """;

        jdbcTemplate.batchUpdate(sql, bills, 500, (ps, bill) -> {
            ps.setLong(1, bill.getOrganizationId());
            ps.setDate(2, java.sql.Date.valueOf(bill.getUsageDate()));
            ps.setString(3, bill.getPayerAccountId());
            ps.setString(4, bill.getUsageAccountId());
            ps.setString(5, bill.getServiceCode());
            ps.setString(6, bill.getServiceName());
            ps.setString(7, bill.getSkuId());
            ps.setString(8, bill.getSkuDescription());
            ps.setString(9, bill.getRegion());
            ps.setString(10, bill.getLocation());
            ps.setString(11, bill.getCurrency());
            ps.setString(12, bill.getPricingType());
            ps.setString(13, bill.getBillingType());
            ps.setString(14, bill.getUsageType());
            ps.setBigDecimal(15, bill.getUsageAmount());
            ps.setString(16, bill.getUsageUnit());
            ps.setBigDecimal(17, bill.getUnblendedCost());
            ps.setBigDecimal(18, bill.getBlendedCost());
            ps.setBigDecimal(19, bill.getNetCost());
        });
    }

    private static BigDecimal makeRound(BigDecimal value) {
        return value.setScale(8, RoundingMode.HALF_UP);
    }

}
