package com.multicloud.batch.service;

import com.multicloud.batch.dto.OrganizationPricingDTO;
import com.multicloud.batch.enums.CloudProvider;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim Molla
 * Email: shamim.molla@vivasoftltd.com
 */

@Slf4j
@Service
@Transactional(readOnly = true)
@RequiredArgsConstructor
public class OrganizationPricingService {

    private static final Map<DiscountGroup, OrganizationPricingDTO> MAP = new HashMap<>();

    private final JdbcTemplate jdbcTemplate;

    public void cacheAllActivePricing() {

        MAP.clear();

        findAllActivePricing().forEach(dto -> MAP.put(
                new DiscountGroup(dto.organizationId(), dto.provider()), dto
        ));

    }

    public OrganizationPricingDTO getPricing(long organizationId, CloudProvider provider) {
        return MAP.get(new DiscountGroup(organizationId, provider));
    }

    private List<OrganizationPricingDTO> findAllActivePricing() {

        return jdbcTemplate.query(
                """
                        SELECT * FROM (
                            SELECT
                                organization_id,
                                cloud_provider,
                                discount,
                                handling_fee,
                                support_fee,
                                start_date,
                                RANK() OVER (PARTITION BY organization_id, cloud_provider ORDER BY start_date DESC) AS rank
                            FROM organization_pricing
                            WHERE start_date <= CURRENT_DATE
                        ) as t WHERE rank = 1;
                        """,
                (rs, rowNum) -> new OrganizationPricingDTO(
                        rs.getLong("organization_id"),
                        CloudProvider.valueOf(rs.getString("cloud_provider")),
                        rs.getDouble("discount"),
                        rs.getDouble("handling_fee"),
                        rs.getDouble("support_fee"),
                        LocalDate.parse(rs.getString("start_date"))
                )
        );
    }

    record DiscountGroup(long organizationId, CloudProvider provider) {
    }

}
