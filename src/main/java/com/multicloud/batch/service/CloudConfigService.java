package com.multicloud.batch.service;

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
 * User: Md. Shamim
 * Date: 8/17/25
 * Email: mdshamim723@gmail.com
 */

@Slf4j
@RequiredArgsConstructor
@Service
@Transactional(readOnly = true)
public class CloudConfigService {

    private final JdbcTemplate jdbcTemplate;

    public Optional<String> getConfigByOrganizationIdAndCloudProvider(long orgId, CloudProvider provider) {

        List<String> results = jdbcTemplate.queryForList(
                "SELECT secret_arn FROM cloud_configs WHERE organization_id = ? AND cloud_provider = ?;",
                String.class,
                orgId, provider.name()
        );

        return results.stream().findFirst();
    }

}
