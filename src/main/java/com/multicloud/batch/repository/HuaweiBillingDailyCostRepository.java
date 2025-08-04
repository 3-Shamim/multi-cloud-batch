package com.multicloud.batch.repository;

import com.multicloud.batch.model.HuaweiBillingDailyCost;
import jakarta.persistence.EntityManager;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Collection;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim Molla
 * Email: shamim.molla@vivasoftltd.com
 */

@Repository
public interface HuaweiBillingDailyCostRepository extends JpaRepository<HuaweiBillingDailyCost, Long> {

    default void upsertHuaweiBillingDailyCosts(Collection<HuaweiBillingDailyCost> bills, EntityManager entityManager) {

        if (bills == null || bills.isEmpty()) return;

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

            sqlBuilder.append(
                    "(%d, '%s', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                            .formatted(
                                    b.getOrganizationId(),
                                    b.getBillDate(),
                                    escapeSql(b.getPayerAccountId()),
                                    escapeSql(b.getCustomerId()),
                                    escapeSql(b.getEnterpriseProjectId()),
                                    escapeSql(b.getEnterpriseProjectName()),
                                    escapeSql(b.getCloudServiceType()),
                                    escapeSql(b.getCloudServiceTypeName()),
                                    escapeSql(b.getSkuCode()),
                                    escapeSql(b.getProductSpecDesc()),
                                    escapeSql(b.getResourceTypeCode()),
                                    escapeSql(b.getResourceTypeName()),
                                    escapeSql(b.getResourceName()),
                                    escapeSql(b.getRegion()),
                                    escapeSql(b.getRegionName()),
                                    b.getChargeMode() != null ? b.getChargeMode() : "NULL",
                                    b.getUsageAmount() != null ? b.getUsageAmount().toPlainString() : "NULL",
                                    b.getConsumeAmount() != null ? b.getConsumeAmount().toPlainString() : "NULL",
                                    b.getOfficialAmount() != null ? b.getOfficialAmount().toPlainString() : "NULL",
                                    b.getDiscountAmount() != null ? b.getDiscountAmount().toPlainString() : "NULL",
                                    b.getCouponAmount() != null ? b.getCouponAmount().toPlainString() : "NULL"
                            )
            );

            i++;

            if (i < bills.size()) {
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

    private static String escapeSql(String value) {
        return value == null ? "" : value.replace("'", "''");
    }

}
