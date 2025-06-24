package com.multicloud.batch.repository;

import com.multicloud.batch.enums.CloudProvider;
import com.multicloud.batch.model.CloudConfig;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim Molla
 * Email: shamim.molla@vivasoftltd.com
 */

@Repository
public interface CloudConfigRepository extends JpaRepository<CloudConfig, Long> {

    Slice<CloudConfig> findAllByCloudProviderAndDisabledFalse(CloudProvider cloudProvider, Pageable pageable);

    Optional<CloudConfig> findByIdAndOrganizationId(long id, long orgId);

    Optional<CloudConfig> findByOrganizationIdAndCloudProvider(long orgId, CloudProvider cloudProvider);

}
