package com.multicloud.batch.repository;

import com.multicloud.batch.enums.CloudProvider;
import com.multicloud.batch.enums.LastSyncStatus;
import com.multicloud.batch.model.DataSyncHistory;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.LocalDate;
import java.util.List;
import java.util.Optional;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim Molla
 * Email: shamim.molla@vivasoftltd.com
 */

public interface DataSyncHistoryRepository extends JpaRepository<DataSyncHistory, Long> {

    @Query("""
            SELECT EXISTS(
                SELECT 1 FROM DataSyncHistory d
                WHERE d.organization.id = :orgId
                AND d.cloudProvider = :cloudProvider
            )
            """)
    boolean existsAny(@Param("orgId") long orgId, @Param("cloudProvider") CloudProvider cloudProvider);

    List<DataSyncHistory> findAllByOrganizationIdAndCloudProviderAndLastSyncStatusAndFailCountLessThan(
            long orgId, CloudProvider cloudProvider, LastSyncStatus lastSyncStatus, int failCount
    );

    Optional<DataSyncHistory> findByOrganizationIdAndCloudProviderAndJobNameAndStartAndEnd(
            long orgId, CloudProvider cloudProvider, String jobName, LocalDate from, LocalDate to
    );


}
