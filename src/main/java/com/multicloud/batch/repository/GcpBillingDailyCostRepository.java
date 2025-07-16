package com.multicloud.batch.repository;

import com.multicloud.batch.model.GcpBillingDailyCost;
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
public interface GcpBillingDailyCostRepository extends JpaRepository<GcpBillingDailyCost, Long> {

    default void upsertGcpBillingDailyCosts(List<GcpBillingDailyCost> bills, EntityManager entityManager) {

        if (bills == null || bills.isEmpty()) return;

        StringBuilder sqlBuilder = new StringBuilder("""
                    INSERT INTO gcp_billing_daily_costs
                    (
                        usage_date, billing_account_id, project_id, project_name, service_code, service_name,
                        sku_id, sku_description, region, location, currency, cost_type,
                        usage_unit, usage_amount, cost, billing_period_start, billing_period_end
                    )
                    VALUES
                """);

        for (int i = 0; i < bills.size(); i++) {

            GcpBillingDailyCost b = bills.get(i);

            sqlBuilder.append(
                    "('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', %s, %s, '%s', '%s')"
                            .formatted(
                                    b.getUsageDate(),
                                    escapeSql(b.getBillingAccountId()),
                                    escapeSql(b.getProjectId()),
                                    escapeSql(b.getProjectName()),
                                    escapeSql(b.getServiceCode()),
                                    escapeSql(b.getServiceName()),
                                    escapeSql(b.getSkuId()),
                                    escapeSql(b.getSkuDescription()),
                                    escapeSql(b.getRegion()),
                                    escapeSql(b.getLocation()),
                                    escapeSql(b.getCurrency()),
                                    escapeSql(b.getCostType()),
                                    escapeSql(b.getUsageUnit()),
                                    b.getUsageAmount() != null ? b.getUsageAmount().toPlainString() : "NULL",
                                    b.getCost() != null ? b.getCost().toPlainString() : "NULL",
                                    b.getBillingPeriodStart(),
                                    b.getBillingPeriodEnd()
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
                        cost = VALUES(cost),
                        billing_period_start = VALUES(billing_period_start),
                        billing_period_end = VALUES(billing_period_end)
                """);

        entityManager.createNativeQuery(sqlBuilder.toString()).executeUpdate();
    }


    private static String escapeSql(String value) {
        return value == null ? "" : value.replace("'", "''");
    }

}
