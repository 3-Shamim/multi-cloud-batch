package com.multicloud.batch.repository;

import com.multicloud.batch.model.HuaweiBillingDailyCost;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.Collection;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim Molla
 * Email: shamim.molla@vivasoftltd.com
 */

@Repository
public interface HuaweiBillingDailyCostRepository extends JpaRepository<HuaweiBillingDailyCost, Long> {

    default void upsertHuaweiBillingDailyCosts(Collection<HuaweiBillingDailyCost> bills, JdbcTemplate jdbcTemplate,
                                               boolean internal) {

        if (bills == null || bills.isEmpty()) {
            return;
        }

        String sql = """
                    INSERT INTO huawei_billing_daily_costs (
                        bill_date, billing_month, payer_account_id, customer_id, enterprise_project_id, enterprise_project_name,
                        cloud_service_type, cloud_service_type_name, sku_code, product_spec_desc,
                        resource_type_code, resource_type_name, resource_name, region, region_name,
                        charge_mode, bill_type, usage_amount, consume_amount, debt_amount, official_amount,
                        ext_consume_amount, ext_debt_amount, ext_official_amount
                    ) VALUES
                    (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON DUPLICATE KEY UPDATE
                        enterprise_project_name = VALUES(enterprise_project_name),
                        cloud_service_type_name = VALUES(cloud_service_type_name),
                        product_spec_desc = VALUES(product_spec_desc),
                        resource_type_name = VALUES(resource_type_name),
                        resource_name = VALUES(resource_name),
                        region_name = VALUES(region_name),
                        usage_amount = VALUES(usage_amount),
                """;

        if (internal) {
            sql += """
                        consume_amount = VALUES(consume_amount),
                        debt_amount = VALUES(debt_amount),
                        official_amount = VALUES(official_amount)
                    """;
        } else {
            sql += """
                        ext_consume_amount = VALUES(ext_consume_amount),
                        ext_debt_amount = VALUES(ext_debt_amount),
                        ext_official_amount = VALUES(ext_official_amount)
                    """;
        }

        jdbcTemplate.batchUpdate(sql, bills, 500, (ps, bill) -> {
            ps.setDate(1, java.sql.Date.valueOf(bill.getBillDate()));
            ps.setDate(2, java.sql.Date.valueOf(bill.getBillingMonth()));
            ps.setString(3, bill.getPayerAccountId());
            ps.setString(4, bill.getCustomerId());
            ps.setString(5, bill.getEnterpriseProjectId());
            ps.setString(6, bill.getEnterpriseProjectName());
            ps.setString(7, bill.getCloudServiceType());
            ps.setString(8, bill.getCloudServiceTypeName());
            ps.setString(9, bill.getSkuCode());
            ps.setString(10, bill.getProductSpecDesc());
            ps.setString(11, bill.getResourceTypeCode());
            ps.setString(12, bill.getResourceTypeName());
            ps.setString(13, bill.getResourceName());
            ps.setString(14, bill.getRegion());
            ps.setString(15, bill.getRegionName());
            ps.setInt(16, bill.getChargeMode());
            ps.setInt(17, bill.getBillType());
            ps.setBigDecimal(18, bill.getUsageAmount());
            ps.setBigDecimal(19, bill.getConsumeAmount());
            ps.setBigDecimal(20, bill.getDebtAmount());
            ps.setBigDecimal(21, bill.getOfficialAmount());
            ps.setBigDecimal(22, bill.getExtConsumeAmount());
            ps.setBigDecimal(23, bill.getExtDebtAmount());
            ps.setBigDecimal(24, bill.getExtOfficialAmount());
        });
    }

}
