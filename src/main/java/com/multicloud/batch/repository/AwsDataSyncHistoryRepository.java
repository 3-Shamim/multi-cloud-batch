package com.multicloud.batch.repository;

import com.multicloud.batch.enums.LastSyncStatus;
import com.multicloud.batch.model.AwsDataSyncHistory;
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

public interface AwsDataSyncHistoryRepository extends JpaRepository<AwsDataSyncHistory, Long> {

    @Query("""
            SELECT EXISTS(
                SELECT 1 FROM AwsDataSyncHistory d WHERE d.jobName = :jobName AND d.tableName = :tableName
            )
            """)
    boolean existsAny(@Param("jobName") String jobName, @Param("tableName") String tableName);

    List<AwsDataSyncHistory> findAllByJobNameAndTableNameAndLastSyncStatusAndFailCountLessThan(
            String jobName, String tableName, LastSyncStatus lastSyncStatus, int failCount
    );

    List<AwsDataSyncHistory> findAllByJobNameAndTableNameInAndLastSyncStatusAndFailCountLessThan(
            String jobName, Set<String> tables, LastSyncStatus lastSyncStatus, int failCount
    );

    Optional<AwsDataSyncHistory> findByJobNameAndTableNameAndStartAndEnd(
            String jobName, String tableName, LocalDate from, LocalDate to
    );


}
