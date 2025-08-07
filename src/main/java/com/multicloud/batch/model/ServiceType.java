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
@AllArgsConstructor
@Getter
@Setter
@ToString
@EqualsAndHashCode(of = "id", callSuper = false)
@Builder
@Entity
@Table(name = "service_types")
public class ServiceType implements Serializable {

    @Serial
    private static final long serialVersionUID = 85463552545863549L;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private long id;

    @NotNull(message = "Code must not be null")
    @Size(min = 1, max = 200, message = "Code must be between {min} to {max}")
    @Column(nullable = false, length = 200)
    private String code;

    @NotNull(message = "Name must not be null")
    @Size(min = 1, max = 200, message = "Name must be between {min} to {max}")
    @Column(nullable = false, length = 200)
    private String name;

    private String abbreviation;

    @NotNull(message = "Parent category must not be null")
    @Size(min = 1, max = 200, message = "Parent category must be between {min} to {max}")
    @Column(name = "parent_category", nullable = false, length = 200)
    private String parentCategory;

    @NotNull(message = "Cloud provider must not be null")
    @Enumerated(EnumType.STRING)
    @Column(name = "cloud_provider", nullable = false, length = 100)
    private CloudProvider cloudProvider;

}
