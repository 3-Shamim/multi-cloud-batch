package com.multicloud.batch.repository;

import com.multicloud.batch.model.AwsBillingDailyCost;
import jakarta.persistence.EntityManager;
import jakarta.persistence.Query;
import org.springframework.data.jpa.repository.JpaRepository;
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

    default void upsertAwsBillingDailyCosts(List<AwsBillingDailyCost> bills, EntityManager entityManager) {

        if (bills == null || bills.isEmpty()) {
            return;
        }

        String sql = """
                    INSERT INTO aws_billing_daily_costs
                    (organization_id, usage_date, payer_account_id, usage_account_id,
                     service_code, service_name, sku_id, sku_description, region, location,
                     currency, pricing_type, billing_type, usage_type, usage_amount, usage_unit,
                     unblended_cost, blended_cost, effective_cost)
                    VALUES
                    (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON DUPLICATE KEY UPDATE
                        currency = VALUES(currency),
                        pricing_type = VALUES(pricing_type),
                        usage_amount = VALUES(usage_amount),
                        usage_unit = VALUES(usage_unit),
                        unblended_cost = VALUES(unblended_cost),
                        blended_cost = VALUES(blended_cost),
                        effective_cost = VALUES(effective_cost)
                """;

        Query query = entityManager.createNativeQuery(sql);

        for (AwsBillingDailyCost b : bills) {
            query.setParameter(1, b.getOrganizationId());
            query.setParameter(2, b.getUsageDate());
            query.setParameter(3, b.getPayerAccountId());
            query.setParameter(4, b.getUsageAccountId());
            query.setParameter(5, b.getServiceCode());
            query.setParameter(6, b.getServiceName());
            query.setParameter(7, b.getSkuId());
            query.setParameter(8, b.getSkuDescription());
            query.setParameter(9, b.getRegion());
            query.setParameter(10, b.getLocation());
            query.setParameter(11, b.getCurrency());
            query.setParameter(12, b.getPricingType());
            query.setParameter(13, b.getBillingType());
            query.setParameter(14, b.getUsageType());
            query.setParameter(15, b.getUsageAmount() != null ? makeRound(b.getUsageAmount()) : null);
            query.setParameter(16, b.getUsageUnit());
            query.setParameter(17, b.getUnblendedCost() != null ? makeRound(b.getUnblendedCost()) : null);
            query.setParameter(18, b.getBlendedCost() != null ? makeRound(b.getBlendedCost()) : null);
            query.setParameter(19, b.getEffectiveCost() != null ? makeRound(b.getEffectiveCost()) : null);

            query.executeUpdate();
        }
    }

    private static BigDecimal makeRound(BigDecimal value) {
        return value.setScale(8, RoundingMode.HALF_UP);
    }

}
