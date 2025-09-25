package com.multicloud.batch.model;

import com.multicloud.batch.enums.LastSyncStatus;
import jakarta.persistence.*;
import lombok.*;

import java.time.LocalDate;
import java.time.LocalDateTime;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim Molla
 * Email: shamim.molla@vivasoftltd.com
 */

@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
@Builder
@ToString
@Entity
@Table(
        name = "aws_data_sync_histories",
        uniqueConstraints = {
                @UniqueConstraint(
                        name = "idx_unq_data_sync_histories",
                        columnNames = {"job_name", "table_name", "start", "end"}
                )
        }
)
public class AwsDataSyncHistory {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "job_name", nullable = false, length = 150)
    private String jobName;

    @Column(name = "table_name", nullable = false, length = 150)
    private String tableName;

    @Column(nullable = false)
    private LocalDate start;

    @Column(nullable = false)
    private LocalDate end;

    @Enumerated(EnumType.STRING)
    @Column(name = "last_sync_status", length = 50)
    private LastSyncStatus lastSyncStatus;

    @Column(name = "fail_count")
    private int failCount;

    @Column(name = "created_at")
    private LocalDateTime createdAt;
    @Column(name = "updated_at")
    private LocalDateTime updatedAt;

    public AwsDataSyncHistory(String jobName, String tableName, LocalDate start, LocalDate end) {
        this.jobName = jobName;
        this.tableName = tableName;
        this.start = start;
        this.end = end;
    }

    @PrePersist
    public void prePersist() {
        this.createdAt = LocalDateTime.now();
    }

    @PreUpdate
    public void preUpdate() {
        this.updatedAt = LocalDateTime.now();
    }

}
