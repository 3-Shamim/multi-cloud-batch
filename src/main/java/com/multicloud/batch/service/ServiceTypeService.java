package com.multicloud.batch.service;

import com.multicloud.batch.dto.ServiceTypeDTO;
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
public class ServiceTypeService {

    private final Map<ServiceTypeGroup, String> SERVICE_TYPE_MAP = new ConcurrentHashMap<>();

    private final JdbcTemplate jdbcTemplate;

    public void fetchAndStoreServiceTypeToMap() {

        SERVICE_TYPE_MAP.clear();

        findAllServiceTypes().forEach(serviceTypeDTO -> SERVICE_TYPE_MAP.put(
                new ServiceTypeGroup(
                        serviceTypeDTO.code(), CloudProvider.valueOf(serviceTypeDTO.cloudProvider())
                ),
                serviceTypeDTO.parentCategory())
        );

    }

    public String getParentCategory(String code, CloudProvider provider) {
        return SERVICE_TYPE_MAP.get(new ServiceTypeGroup(code, provider));
    }

    private List<ServiceTypeDTO> findAllServiceTypes() {

        return jdbcTemplate.query(
                "SELECT code, cloud_provider, parent_category FROM service_types",
                (rs, rowNum) -> new ServiceTypeDTO(
                        rs.getString("code"),
                        rs.getString("cloud_provider"),
                        rs.getString("parent_category")
                )
        );
    }

    private record ServiceTypeGroup(String code, CloudProvider provider) {
    }

}
