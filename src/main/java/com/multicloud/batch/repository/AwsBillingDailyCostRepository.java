package com.multicloud.batch.repository;

import com.multicloud.batch.model.AwsBillingDailyCost;
import jakarta.persistence.EntityManager;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

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

        StringBuilder sqlBuilder = new StringBuilder("""
                    INSERT INTO aws_billing_daily_costs
                    (
                        organization_id, usage_date, payer_account_id, usage_account_id,
                        service_code, service_name, sku_id, sku_description, region, location,
                        currency, pricing_type, usage_type, usage_amount, usage_unit,
                        unblended_cost, blended_cost, effective_cost
                    )
                    VALUES
                """);

        for (int i = 0; i < bills.size(); i++) {

            AwsBillingDailyCost b = bills.get(i);

            sqlBuilder.append(
                    "(%d, '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', %s, '%s', %s, %s, %s)"
                            .formatted(
                                    b.getOrganizationId(),
                                    b.getUsageDate(),
                                    escapeSql(b.getPayerAccountId()),
                                    escapeSql(b.getUsageAccountId()),
                                    escapeSql(b.getServiceCode()),
                                    escapeSql(b.getServiceName()),
                                    escapeSql(b.getSkuId()),
                                    escapeSql(b.getSkuDescription()),
                                    escapeSql(b.getRegion()),
                                    escapeSql(b.getLocation()),
                                    escapeSql(b.getCurrency()),
                                    escapeSql(b.getPricingType()),
                                    escapeSql(b.getUsageType()),
                                    b.getUsageAmount() != null ? b.getUsageAmount().toPlainString() : "NULL",
                                    escapeSql(b.getUsageUnit()),
                                    b.getUnblendedCost() != null ? b.getUnblendedCost().toPlainString() : "NULL",
                                    b.getBlendedCost() != null ? b.getBlendedCost().toPlainString() : "NULL",
                                    b.getEffectiveCost() != null ? b.getEffectiveCost().toPlainString() : "NULL"
                            )
            );

            if (i < bills.size() - 1) {
                sqlBuilder.append(", ");
            }

        }

        sqlBuilder.append("""
                    ON DUPLICATE KEY UPDATE
                        usage_amount = VALUES(usage_amount),
                        usage_unit = VALUES(usage_unit),
                        unblended_cost = VALUES(unblended_cost),
                        blended_cost = VALUES(blended_cost),
                        effective_cost = VALUES(effective_cost)
                """);

        entityManager.createNativeQuery(sqlBuilder.toString()).executeUpdate();
    }

    private String escapeSql(String value) {
        if (value == null) return "";
        return value.replace("'", "''");
    }

}
