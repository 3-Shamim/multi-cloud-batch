package com.multicloud.batch.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.multicloud.batch.enums.CloudProvider;
import jakarta.persistence.*;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
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

    private String accessKey;
    private String secretKey;

    @Column(columnDefinition = "boolean default false")
    private boolean disabled;
    @Column(columnDefinition = "boolean default false")
    private boolean connected;

    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime lastSyncTime;

}
