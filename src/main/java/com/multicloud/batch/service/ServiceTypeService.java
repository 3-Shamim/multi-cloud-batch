package com.multicloud.batch.service;

import com.multicloud.batch.enums.CloudProvider;
import com.multicloud.batch.repository.ServiceTypeRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

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
public class ServiceTypeService {

    private final Map<ServiceTypeGroup, String> SERVICE_TYPE_MAP = new ConcurrentHashMap<>();

    private final ServiceTypeRepository serviceTypeRepository;

    public void fetchAndStoreServiceTypeMap() {

        SERVICE_TYPE_MAP.clear();

        serviceTypeRepository.findAll().forEach(serviceType -> SERVICE_TYPE_MAP.put(
                new ServiceTypeGroup(serviceType.getCode(), serviceType.getCloudProvider()),
                serviceType.getParentCategory())
        );

    }

    public String getParentCategory(String code, CloudProvider provider) {
        return SERVICE_TYPE_MAP.get(new ServiceTypeGroup(code, provider));
    }

    private record ServiceTypeGroup(String code, CloudProvider provider) {
    }

}
