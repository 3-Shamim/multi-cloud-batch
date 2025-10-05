package com.multicloud.batch.service;

import com.multicloud.batch.dto.HuaweiSubAccountInfoDTO;
import com.multicloud.batch.enums.CloudProvider;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim Molla
 * Email: shamim.molla@vivasoftltd.com
 */

@Slf4j
@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class HuaweiSubAccountInfoService {

    private final JdbcTemplate jdbcTemplate;

    public List<HuaweiSubAccountInfoDTO> findAllSubAccountInfo() {

        return jdbcTemplate.query(
                "SELECT domain_name, agency_name, project FROM huawei_sub_account_info",
                (rs, rowNum) -> new HuaweiSubAccountInfoDTO(
                        rs.getString("domain_name"),
                        rs.getString("agency_name"),
                        rs.getString("project")
                )
        );
    }

}
