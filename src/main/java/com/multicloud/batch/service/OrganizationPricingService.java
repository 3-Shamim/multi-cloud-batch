package com.multicloud.batch.service;

import com.multicloud.batch.dto.OrganizationPricingDTO;
import com.multicloud.batch.enums.CloudProvider;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDate;
import java.util.Comparator;
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

    private final Map<DiscountGroup, Double> DISCOUNT_CACHE = new HashMap<>();

    private final JdbcTemplate jdbcTemplate;

    public void cachePerDayDiscount(Long orgId, CloudProvider provider, LocalDate startDate, LocalDate endDate) {

        DISCOUNT_CACHE.clear();

        List<OrganizationPricingDTO> discounts = findOrganizationDiscountByProviderAndUpToDate(orgId, provider, endDate);

        LocalDate current = startDate;

        while (!current.isAfter(endDate)) {

            final LocalDate finalCurrent = current;

            Double activeDiscount = discounts.stream()
                    .filter(d -> !d.startDate().isAfter(finalCurrent))
                    .max(Comparator.comparing(OrganizationPricingDTO::startDate))
                    .map(OrganizationPricingDTO::discount)
                    .orElse(null);

            DISCOUNT_CACHE.put(new DiscountGroup(provider, current), activeDiscount);

            current = current.plusDays(1);
        }

    }

    public Double getDiscountByDate(CloudProvider provider, LocalDate date) {
        return DISCOUNT_CACHE.get(new DiscountGroup(provider, date));
    }

    private List<OrganizationPricingDTO> findOrganizationDiscountByProviderAndUpToDate(long orgId, CloudProvider provider, LocalDate endDate) {

        return jdbcTemplate.query(
                """
                        SELECT organization_id, start_date, discount, service_fee
                        FROM organization_pricing
                        WHERE organization_id = ?
                            AND cloud_provider = ?
                            AND start_date <= ?;
                        """,
                (rs, rowNum) -> new OrganizationPricingDTO(
                        rs.getLong("organization_id"),
                        LocalDate.parse(rs.getString("start_date")),
                        rs.getDouble("discount"),
                        rs.getDouble("service_fee")
                ),
                orgId,
                provider.name(),
                endDate
        );
    }

    record DiscountGroup(CloudProvider provider, LocalDate startDate) {
    }

}
