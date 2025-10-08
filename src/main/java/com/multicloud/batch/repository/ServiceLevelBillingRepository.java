package com.multicloud.batch.repository;

import com.multicloud.batch.helper.ServiceLevelBillingSql;
import com.multicloud.batch.model.ServiceLevelBilling;
import org.springframework.batch.item.Chunk;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim Molla
 * Email: shamim.molla@vivasoftltd.com
 */

public interface ServiceLevelBillingRepository extends JpaRepository<ServiceLevelBilling, Long> {

    default void upsert(Chunk<? extends ServiceLevelBilling> records, long orgId, JdbcTemplate jdbcTemplate) {

        jdbcTemplate.batchUpdate(
                ServiceLevelBillingSql.UPSERT_SQL,
                new BatchPreparedStatementSetter() {

                    @Override
                    public void setValues(PreparedStatement ps, int i) throws SQLException {

                        ServiceLevelBilling item = records.getItems().get(i);

                        ps.setLong(1, orgId);
                        ps.setDate(2, Date.valueOf(item.getUsageDate()));
                        ps.setString(3, item.getCloudProvider().name());
                        ps.setString(4, item.getBillingAccountId());
                        ps.setString(5, item.getUsageAccountId());
                        ps.setString(6, item.getUsageAccountName());
                        ps.setString(7, item.getServiceCode());
                        ps.setString(8, item.getServiceName());
                        ps.setString(9, item.getBillingType());
                        ps.setString(10, item.getParentCategory());
                        ps.setBigDecimal(11, item.getCost());
                        ps.setBigDecimal(12, item.getFinalCost());

                    }

                    @Override
                    public int getBatchSize() {
                        return records.size();
                    }

                }
        );

    }

}
