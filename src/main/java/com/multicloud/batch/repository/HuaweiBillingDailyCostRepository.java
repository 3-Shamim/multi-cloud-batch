package com.multicloud.batch.repository;

import com.multicloud.batch.model.HuaweiBillingDailyCost;
import jakarta.persistence.EntityManager;
import jakarta.persistence.Query;
import org.springframework.data.jpa.repository.JpaRepository;
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

    default void upsertHuaweiBillingDailyCosts(Collection<HuaweiBillingDailyCost> bills, EntityManager entityManager) {

        if (bills == null || bills.isEmpty()) {
            return;
        }

        String sql = """
                    INSERT INTO huawei_billing_daily_costs (
                        organization_id, bill_date, payer_account_id, customer_id, enterprise_project_id,
                        enterprise_project_name, cloud_service_type, cloud_service_type_name, sku_code,
                        product_spec_desc, resource_type_code, resource_type_name, resource_name, region,
                        region_name, charge_mode, bill_type, usage_amount, consume_amount, official_amount,
                        discount_amount, coupon_amount
                    ) VALUES
                    (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON DUPLICATE KEY UPDATE
                        enterprise_project_name = VALUES(enterprise_project_name),
                        cloud_service_type_name = VALUES(cloud_service_type_name),
                        product_spec_desc = VALUES(product_spec_desc),
                        resource_type_name = VALUES(resource_type_name),
                        resource_name = VALUES(resource_name),
                        region_name = VALUES(region_name),
                        usage_amount = VALUES(usage_amount),
                        consume_amount = VALUES(consume_amount),
                        official_amount = VALUES(official_amount),
                        discount_amount = VALUES(discount_amount),
                        coupon_amount = VALUES(coupon_amount)
                """;

        Query query = entityManager.createNativeQuery(sql);

        for (HuaweiBillingDailyCost b : bills) {
            query.setParameter(1, b.getOrganizationId());
            query.setParameter(2, b.getBillDate());
            query.setParameter(3, b.getPayerAccountId());
            query.setParameter(4, b.getCustomerId());
            query.setParameter(5, b.getEnterpriseProjectId());
            query.setParameter(6, b.getEnterpriseProjectName());
            query.setParameter(7, b.getCloudServiceType());
            query.setParameter(8, b.getCloudServiceTypeName());
            query.setParameter(9, b.getSkuCode());
            query.setParameter(10, b.getProductSpecDesc());
            query.setParameter(11, b.getResourceTypeCode());
            query.setParameter(12, b.getResourceTypeName());
            query.setParameter(13, b.getResourceName());
            query.setParameter(14, b.getRegion());
            query.setParameter(15, b.getRegionName());
            query.setParameter(16, b.getChargeMode());
            query.setParameter(17, b.getBillType());
            query.setParameter(18, b.getUsageAmount() != null ? makeRound(b.getUsageAmount()) : null);
            query.setParameter(19, b.getConsumeAmount() != null ? makeRound(b.getConsumeAmount()) : null);
            query.setParameter(20, b.getOfficialAmount() != null ? makeRound(b.getOfficialAmount()) : null);
            query.setParameter(21, b.getDiscountAmount() != null ? makeRound(b.getDiscountAmount()) : null);
            query.setParameter(22, b.getCouponAmount() != null ? makeRound(b.getCouponAmount()) : null);

            query.executeUpdate();
        }

    }

    private static BigDecimal makeRound(BigDecimal value) {
        return value.setScale(8, RoundingMode.HALF_UP);
    }

}
