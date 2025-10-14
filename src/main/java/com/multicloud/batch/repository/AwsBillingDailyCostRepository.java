package com.multicloud.batch.repository;

import com.multicloud.batch.model.AwsBillingDailyCost;
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

    default void upsertAwsBillingDailyCosts(List<AwsBillingDailyCost> bills, JdbcTemplate jdbcTemplate, boolean internal) {

        String sql = """
                    INSERT INTO aws_billing_daily_costs
                    (usage_date, payer_account_id, usage_account_id,
                     service_code, service_name, sku_id, region, location,
                     currency, pricing_type, billing_type, usage_type, usage_amount, usage_unit,
                     unblended_cost, blended_cost, ext_unblended_cost, ext_blended_cost)
                    VALUES
                    (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON DUPLICATE KEY UPDATE
                        service_name = VALUES(service_name),
                        location = VALUES(location),
                        currency = VALUES(currency),
                        pricing_type = VALUES(pricing_type),
                        usage_amount = VALUES(usage_amount),
                        usage_unit = VALUES(usage_unit),
                """;

        if (internal) {
            sql += """
                        unblended_cost = VALUES(unblended_cost),
                        blended_cost = VALUES(blended_cost)
                    """;
        } else {
            sql += """
                        ext_unblended_cost = VALUES(ext_unblended_cost),
                        ext_blended_cost = VALUES(ext_blended_cost)
                    """;
        }

        jdbcTemplate.batchUpdate(sql, bills, 500, (ps, bill) -> {
            ps.setDate(1, java.sql.Date.valueOf(bill.getUsageDate()));
            ps.setString(2, bill.getPayerAccountId());
            ps.setString(3, bill.getUsageAccountId());
            ps.setString(4, bill.getServiceCode());
            ps.setString(5, bill.getServiceName());
            ps.setString(6, bill.getSkuId());
            ps.setString(7, bill.getRegion());
            ps.setString(8, bill.getLocation());
            ps.setString(9, bill.getCurrency());
            ps.setString(10, bill.getPricingType());
            ps.setString(11, bill.getBillingType());
            ps.setString(12, bill.getUsageType());
            ps.setBigDecimal(13, bill.getUsageAmount());
            ps.setString(14, bill.getUsageUnit());
            ps.setBigDecimal(15, bill.getUnblendedCost());
            ps.setBigDecimal(16, bill.getBlendedCost());
            ps.setBigDecimal(17, bill.getExtUnblendedCost());
            ps.setBigDecimal(18, bill.getExtBlendedCost());
        });
    }

    private static BigDecimal makeRound(BigDecimal value) {
        return value.setScale(8, RoundingMode.HALF_UP);
    }

}
