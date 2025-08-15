package com.multicloud.batch.model;

import com.multicloud.batch.enums.CloudProvider;
import jakarta.persistence.*;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import lombok.*;

import java.io.Serial;
import java.io.Serializable;

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

    @NotNull(message = "Organization ID must not be null")
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(
            name = "organization_id",
            nullable = false,
            foreignKey = @ForeignKey(name = "fk_cloud_configs_organization_id")
    )
    private Organization organization;

    @NotNull(message = "Cloud provider must not be null")
    @Enumerated(EnumType.STRING)
    @Column(name = "cloud_provider", nullable = false, length = 100)
    private CloudProvider cloudProvider;

    @Size(min = 1, max = 500, message = "Access key must be between {min} to {max}")
    @Column(name = "access_key", length = 500)
    private String accessKey;

    @Size(min = 1, max = 500, message = "Secret key must be between {min} to {max}")
    @Column(name = "secret_key", length = 500)
    private String secretKey;

    @Lob
    @Column(columnDefinition = "LONGBLOB")
    private byte[] file;

    @Size(min = 1, max = 500, message = "File name must be between {min} to {max}")
    @Column(name = "file_name", length = 500)
    private String fileName;

    @Column(columnDefinition = "boolean default false")
    private boolean disabled;

    @Column(columnDefinition = "boolean default false")
    private boolean connected;

}

