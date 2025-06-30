package com.multicloud.batch.repository;

import com.multicloud.batch.model.CloudDailyBilling;
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
public interface CloudDailyBillingRepository extends JpaRepository<CloudDailyBilling, Long> {

    default void batchUpsert(List<CloudDailyBilling> bills, EntityManager entityManager) {

        if (bills == null || bills.isEmpty()) return;

        StringBuilder sqlBuilder = new StringBuilder("""
                INSERT INTO cloud_daily_billings
                (organization_id, cloud_provider, account_id, project_id, service_name, date,
                 cost_amount_usd, currency, billing_export_source) VALUES
                """).append(" ");

        for (int i = 0; i < bills.size(); i++) {

            CloudDailyBilling b = bills.get(i);

            sqlBuilder.append(String.format(
                    "('%d', '%s', '%s', '%s', '%s', '%s', %s, '%s', '%s')",
                    b.getOrganizationId(),
                    b.getCloudProvider().name(),
                    escapeSql(b.getAccountId()),
                    escapeSql(b.getProjectId()),
                    escapeSql(b.getServiceName()),
                    b.getDate(),
                    b.getCostAmountUsd() != null ? b.getCostAmountUsd().toPlainString() : "NULL",
                    escapeSql(b.getCurrency()),
                    escapeSql(b.getBillingExportSource())
            ));

            if (i < bills.size() - 1) {
                sqlBuilder.append(", ");
            }

        }

        sqlBuilder.append("""
                ON DUPLICATE KEY UPDATE
                    cost_amount_usd = VALUES(cost_amount_usd),
                    currency = VALUES(currency)
                """);

        entityManager.createNativeQuery(sqlBuilder.toString()).executeUpdate();
    }

    private static String escapeSql(String value) {
        return value == null ? null : value.replace("'", "''");
    }

}
