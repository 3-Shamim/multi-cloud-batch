package com.multicloud.batch.service;

import com.multicloud.batch.dto.CloudConfigDTO;
import com.multicloud.batch.enums.CloudProvider;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Optional;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim Molla
 * Email: shamim.molla@vivasoftltd.com
 */

@Slf4j
@RequiredArgsConstructor
@Service
@Transactional(readOnly = true)
public class CloudConfigService {

    private final JdbcTemplate jdbcTemplate;

    public Optional<CloudConfigDTO> getConfigByOrganizationIdAndCloudProvider(long orgId, CloudProvider provider) {

        List<CloudConfigDTO> results = jdbcTemplate.query(
                "SELECT secret_arn, disabled FROM cloud_configs WHERE organization_id = ? AND cloud_provider = ?;",
                (rs, rowNum) -> new CloudConfigDTO(
                        rs.getString("secret_arn"), rs.getBoolean("disabled")
                ),
                orgId, provider.name()
        );

        return results.stream().findFirst();
    }

}
