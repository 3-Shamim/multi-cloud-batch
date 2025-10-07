package com.multicloud.batch.service;

import com.multicloud.batch.enums.CloudProvider;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim Molla
 * Email: shamim.molla@vivasoftltd.com
 */

@Slf4j
@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class ProductAccountService {

    private final JdbcTemplate jdbcTemplate;

    public List<String> findAccountIds(long orgId, CloudProvider provider) {

        return jdbcTemplate.queryForList(
                "SELECT account_id FROM product_accounts WHERE organization_id = ? AND cloud_provider = ?",
                String.class,
                orgId,
                provider.name()
        );
    }

}
