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
        name = "huawei_data_sync_histories",
        uniqueConstraints = {
                @UniqueConstraint(
                        name = "idx_unq_huawei_data_sync_histories",
                        columnNames = {"job_name", "project", "start", "end"}
                )
        },
        indexes = {
                @Index(name = "idx_job_project_huawei_data_sync_histories", columnList = "job_name, project"),
                @Index(name = "idx_project_huawei_data_sync_histories", columnList = "project")

        }
)
public class HuaweiDataSyncHistory {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private long id;

    @Column(name = "job_name", nullable = false, length = 150)
    private String jobName;

    @Column(name = "project", nullable = false)
    private String project;

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

    public HuaweiDataSyncHistory(String jobName, String project, LocalDate start, LocalDate end) {
        this.jobName = jobName;
        this.project = project;
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
