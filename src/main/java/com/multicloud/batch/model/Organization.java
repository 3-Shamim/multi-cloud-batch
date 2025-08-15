package com.multicloud.batch.model;

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
@Table(
        name = "organizations",
        uniqueConstraints = @UniqueConstraint(name = "idx_uq_organizations_name", columnNames = {"name"})
)
public class Organization implements Serializable {

    @Serial
    private static final long serialVersionUID = 543645623445863549L;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private long id;

    @NotNull(message = "Name must not be null")
    @Size(min = 1, max = 100, message = "Name must be between {min} to {max}")
    @Column(nullable = false, length = 100)
    private String name;

    @Column(columnDefinition = "boolean default false")
    private boolean disabled;

}
