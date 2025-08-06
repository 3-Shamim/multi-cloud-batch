package com.multicloud.batch.model;

import com.multicloud.batch.enums.CloudProvider;
import com.multicloud.batch.enums.LastSyncStatus;
import jakarta.persistence.*;
import jakarta.validation.constraints.NotNull;
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
        name = "data_sync_histories",
        uniqueConstraints = {
                @UniqueConstraint(
                        name = "idx_unq_data_sync_histories",
                        columnNames = {"organization_id", "cloud_provider", "job_name", "start", "end"}
                )
        }
)
public class DataSyncHistory {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    // Multicloud organization ID
    @NotNull(message = "Organization ID must not be null")
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "organization_id", nullable = false)
    private Organization organization;

    @Enumerated(EnumType.STRING)
    @Column(name = "cloud_provider", nullable = false, length = 100)
    private CloudProvider cloudProvider;

    @Column(name = "job_name", nullable = false, length = 150)
    private String jobName;

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

    public DataSyncHistory(Organization organization, CloudProvider cloudProvider, String jobName, LocalDate start, LocalDate end) {
        this.organization = organization;
        this.cloudProvider = cloudProvider;
        this.jobName = jobName;
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
