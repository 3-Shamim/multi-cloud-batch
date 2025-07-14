package com.multicloud.batch.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.multicloud.batch.enums.CloudProvider;
import com.multicloud.batch.enums.LastSyncStatus;
import jakarta.persistence.*;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Size;
import lombok.*;

import java.io.Serial;
import java.io.Serializable;
import java.time.LocalDateTime;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim Molla
 * Email: shamim.molla@vivasoftltd.com
 */

@NoArgsConstructor
@Getter
@Setter
@ToString
@EqualsAndHashCode(of = "id", callSuper = false)
@Entity
@Table(name = "cloud_configs")
public class CloudConfig implements Serializable {

    @Serial
    private static final long serialVersionUID = 3564984654148475674L;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private long id;

    @Positive(message = "Organization ID must be a positive number")
    @Column(nullable = false)
    private long organizationId;

    @NotNull(message = "Cloud provider must not be null")
    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 100)
    private CloudProvider cloudProvider;

    @Size(min = 1, max = 500, message = "Access key must be between {min} to {max}")
    @Column(length = 500)
    private String accessKey;

    @Size(min = 1, max = 500, message = "Secret key must be between {min} to {max}")
    @Column(length = 500)
    private String secretKey;

    @Lob
    @Column(columnDefinition = "LONGBLOB")
    private byte[] file;

    @Size(min = 1, max = 500, message = "File name must be between {min} to {max}")
    @Column(length = 500)
    private String fileName;

    @Column(columnDefinition = "boolean default false")
    private boolean disabled;

    @Column(columnDefinition = "boolean default false")
    private boolean connected;

    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime lastSuccessSyncTime;

    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime lastSyncTime;

    @Enumerated(EnumType.STRING)
    @Column(length = 50)
    private LastSyncStatus lastSyncStatus;

    @Column(length = 1000, columnDefinition = "TEXT")
    private String lastSyncMessage;

}
