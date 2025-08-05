package com.multicloud.batch.repository;

import com.multicloud.batch.model.HuaweiBillingDailyCost;
import jakarta.persistence.EntityManager;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.Collection;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim Molla
 * Email: shamim.molla@vivasoftltd.com
 */

@Repository
public interface HuaweiBillingDailyCostRepository extends JpaRepository<HuaweiBillingDailyCost, Long> {

    default void upsertHuaweiBillingDailyCosts(Collection<HuaweiBillingDailyCost> bills, EntityManager entityManager) {

        if (bills == null || bills.isEmpty()) {
            return;
        }

        StringBuilder sqlBuilder = new StringBuilder("""
                    INSERT INTO huawei_billing_daily_costs (
                        organization_id, bill_date, payer_account_id, customer_id, enterprise_project_id,
                        enterprise_project_name, cloud_service_type, cloud_service_type_name, sku_code,
                        product_spec_desc, resource_type_code, resource_type_name, resource_name, region,
                        region_name, charge_mode, usage_amount, consume_amount, official_amount,
                        discount_amount, coupon_amount
                    ) VALUES
                """);

        int i = 0;

        for (HuaweiBillingDailyCost b : bills) {

            sqlBuilder.append("(")
                    .append(b.getOrganizationId()).append(", ")
                    .append(toSqlDate(b.getBillDate())).append(", ")
                    .append(toSqlStr(b.getPayerAccountId())).append(", ")
                    .append(toSqlStr(b.getCustomerId())).append(", ")
                    .append(toSqlStr(b.getEnterpriseProjectId())).append(", ")
                    .append(toSqlStr(b.getEnterpriseProjectName())).append(", ")
                    .append(toSqlStr(b.getCloudServiceType())).append(", ")
                    .append(toSqlStr(b.getCloudServiceTypeName())).append(", ")
                    .append(toSqlStr(b.getSkuCode())).append(", ")
                    .append(toSqlStr(b.getProductSpecDesc())).append(", ")
                    .append(toSqlStr(b.getResourceTypeCode())).append(", ")
                    .append(toSqlStr(b.getResourceTypeName())).append(", ")
                    .append(toSqlStr(b.getResourceName())).append(", ")
                    .append(toSqlStr(b.getRegion())).append(", ")
                    .append(toSqlStr(b.getRegionName())).append(", ")
                    .append(toSqlStr(b.getChargeMode())).append(", ")
                    .append(toSqlDecimal(b.getUsageAmount())).append(", ")
                    .append(toSqlDecimal(b.getConsumeAmount())).append(", ")
                    .append(toSqlDecimal(b.getOfficialAmount())).append(", ")
                    .append(toSqlDecimal(b.getDiscountAmount())).append(", ")
                    .append(toSqlDecimal(b.getCouponAmount()))
                    .append(")");

            if (++i < bills.size()) {
                sqlBuilder.append(", ");
            }

        }

        sqlBuilder.append("""
                    ON DUPLICATE KEY UPDATE
                        usage_amount = VALUES(usage_amount),
                        consume_amount = VALUES(consume_amount),
                        official_amount = VALUES(official_amount),
                        discount_amount = VALUES(discount_amount),
                        coupon_amount = VALUES(coupon_amount)
                """);

        entityManager.createNativeQuery(sqlBuilder.toString()).executeUpdate();
    }

    private static String toSqlStr(String value) {
        return value == null ? "NULL" : "'" + value.replace("'", "''") + "'";
    }

    private static String toSqlDate(LocalDate date) {
        return date == null ? "NULL" : "'" + date + "'";
    }

    private static String toSqlDecimal(BigDecimal value) {
        return value == null ? "NULL" : value.toPlainString();
    }

    private static String toSqlInt(Integer value) {
        return value == null ? "NULL" : value.toString();
    }

}
