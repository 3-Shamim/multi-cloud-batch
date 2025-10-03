package com.multicloud.batch.repository;

import com.multicloud.batch.model.HuaweiBillingDailyCost;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Collection;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim Molla
 * Email: shamim.molla@vivasoftltd.com
 */

@Repository
public interface HuaweiBillingDailyCostRepository extends JpaRepository<HuaweiBillingDailyCost, Long> {

    default void upsertHuaweiBillingDailyCosts(Collection<HuaweiBillingDailyCost> bills, JdbcTemplate jdbcTemplate) {

        if (bills == null || bills.isEmpty()) {
            return;
        }

        String sql = """
                    INSERT INTO huawei_billing_daily_costs (
                        bill_date, payer_account_id, customer_id, enterprise_project_id,
                        enterprise_project_name, cloud_service_type, cloud_service_type_name, sku_code,
                        product_spec_desc, resource_type_code, resource_type_name, resource_name, region,
                        region_name, charge_mode, bill_type, usage_amount, consume_amount, cash_amount,
                        credit_amount, coupon_amount, flexipurchase_coupon_amount, stored_card_amount,
                        bonus_amount, debt_amount, adjustment_amount, official_amount, discount_amount
                    ) VALUES
                    (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON DUPLICATE KEY UPDATE
                        enterprise_project_name = VALUES(enterprise_project_name),
                        cloud_service_type_name = VALUES(cloud_service_type_name),
                        product_spec_desc = VALUES(product_spec_desc),
                        resource_type_name = VALUES(resource_type_name),
                        resource_name = VALUES(resource_name),
                        region_name = VALUES(region_name),
                        usage_amount = VALUES(usage_amount),
                        consume_amount = VALUES(consume_amount),
                        cash_amount = VALUES(cash_amount),
                        credit_amount = VALUES(credit_amount),
                        coupon_amount = VALUES(coupon_amount),
                        flexipurchase_coupon_amount = VALUES(flexipurchase_coupon_amount),
                        stored_card_amount = VALUES(stored_card_amount),
                        bonus_amount = VALUES(bonus_amount),
                        debt_amount = VALUES(debt_amount),
                        adjustment_amount = VALUES(adjustment_amount),
                        official_amount = VALUES(official_amount),
                        discount_amount = VALUES(discount_amount)
                """;

        jdbcTemplate.batchUpdate(sql, bills, 500, (ps, bill) -> {
            ps.setDate(1, java.sql.Date.valueOf(bill.getBillDate()));
            ps.setString(2, bill.getPayerAccountId());
            ps.setString(3, bill.getCustomerId());
            ps.setString(4, bill.getEnterpriseProjectId());
            ps.setString(5, bill.getEnterpriseProjectName());
            ps.setString(6, bill.getCloudServiceType());
            ps.setString(7, bill.getCloudServiceTypeName());
            ps.setString(8, bill.getSkuCode());
            ps.setString(9, bill.getProductSpecDesc());
            ps.setString(10, bill.getResourceTypeCode());
            ps.setString(11, bill.getResourceTypeName());
            ps.setString(12, bill.getResourceName());
            ps.setString(13, bill.getRegion());
            ps.setString(14, bill.getRegionName());
            ps.setInt(15, bill.getChargeMode());
            ps.setInt(16, bill.getBillType());
            ps.setBigDecimal(17, bill.getUsageAmount());
            ps.setBigDecimal(18, bill.getConsumeAmount());
            ps.setBigDecimal(19, bill.getCashAmount());
            ps.setBigDecimal(20, bill.getCreditAmount());
            ps.setBigDecimal(21, bill.getCouponAmount());
            ps.setBigDecimal(22, bill.getFlexipurchaseCouponAmount());
            ps.setBigDecimal(23, bill.getStoredCardAmount());
            ps.setBigDecimal(24, bill.getBonusAmount());
            ps.setBigDecimal(25, bill.getDebtAmount());
            ps.setBigDecimal(26, bill.getAdjustmentAmount());
            ps.setBigDecimal(27, bill.getOfficialAmount());
            ps.setBigDecimal(28, bill.getDiscountAmount());
        });
    }

    private static BigDecimal makeRound(BigDecimal value) {
        return value.setScale(8, RoundingMode.HALF_UP);
    }

}
