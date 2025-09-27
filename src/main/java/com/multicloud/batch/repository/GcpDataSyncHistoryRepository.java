package com.multicloud.batch.repository;

import com.multicloud.batch.enums.LastSyncStatus;
import com.multicloud.batch.model.GcpDataSyncHistory;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.LocalDate;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim Molla
 * Email: shamim.molla@vivasoftltd.com
 */

public interface GcpDataSyncHistoryRepository extends JpaRepository<GcpDataSyncHistory, Long> {

    @Query("""
            SELECT EXISTS(
                SELECT 1 FROM GcpDataSyncHistory d WHERE d.jobName = :jobName
            )
            """)
    boolean existsAny(@Param("jobName") String jobName);

    List<GcpDataSyncHistory> findAllByJobNameAndProjectAndLastSyncStatusAndFailCountLessThan(
            String jobName, String project, LastSyncStatus lastSyncStatus, int failCount
    );

    List<GcpDataSyncHistory> findAllByJobNameAndProjectInAndLastSyncStatusAndFailCountLessThan(
            String jobName, Set<String> projects, LastSyncStatus lastSyncStatus, int failCount
    );

    Optional<GcpDataSyncHistory> findByJobNameAndProjectAndStartAndEnd(
            String jobName, String project, LocalDate from, LocalDate to
    );

}
