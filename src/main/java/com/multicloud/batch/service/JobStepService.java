package com.multicloud.batch.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim Molla
 * Email: shamim.molla@vivasoftltd.com
 */

@Slf4j
@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class JobStepService {

    private final JdbcTemplate jdbcTemplate;

    public boolean hasStepEverCompleted(String stepName) {

        String sql = "SELECT EXISTS (SELECT 1 FROM BATCH_STEP_EXECUTION WHERE STEP_NAME = ? AND STATUS = 'COMPLETED')";

        Boolean exists = jdbcTemplate.queryForObject(sql, Boolean.class, stepName);

        return exists != null && exists;
    }

}
