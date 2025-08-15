package com.multicloud.batch.repository;

import com.multicloud.batch.model.GcpBillingDailyCost;
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
public interface GcpBillingDailyCostRepository extends JpaRepository<GcpBillingDailyCost, Long> {

    default void upsertGcpBillingDailyCosts(List<GcpBillingDailyCost> bills, EntityManager entityManager) {

        if (bills == null || bills.isEmpty()) {
            return;
        }

        String sql = """
                    INSERT INTO gcp_billing_daily_costs
                    (organization_id, usage_date, billing_account_id, project_id, project_name,
                     service_code, service_name, sku_id, sku_description, region, location,
                     currency, cost_type, usage_amount, usage_unit, cost)
                    VALUES
                    (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON DUPLICATE KEY UPDATE
                        currency = VALUES(currency),
                        usage_amount = VALUES(usage_amount),
                        usage_unit = VALUES(usage_unit),
                        cost = VALUES(cost)
                """;

        Query query = entityManager.createNativeQuery(sql);

        for (GcpBillingDailyCost b : bills) {
            query.setParameter(1, b.getOrganizationId());
            query.setParameter(2, b.getUsageDate());
            query.setParameter(3, b.getBillingAccountId());
            query.setParameter(4, b.getProjectId());
            query.setParameter(5, b.getProjectName());
            query.setParameter(6, b.getServiceCode());
            query.setParameter(7, b.getServiceName());
            query.setParameter(8, b.getSkuId());
            query.setParameter(9, b.getSkuDescription());
            query.setParameter(10, b.getRegion());
            query.setParameter(11, b.getLocation());
            query.setParameter(12, b.getCurrency());
            query.setParameter(13, b.getCostType());
            query.setParameter(14, b.getUsageAmount());
            query.setParameter(15, b.getUsageUnit());
            query.setParameter(16, b.getCost() != null ? makeRound(b.getCost()) : null);

            query.executeUpdate();
        }

    }

    private static BigDecimal makeRound(BigDecimal value) {
        return value.setScale(8, RoundingMode.HALF_UP);
    }

}
