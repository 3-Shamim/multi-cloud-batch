package com.multicloud.batch.dao.huawei;

import com.multicloud.batch.dao.huawei.payload.HuaweiBillingGroup;
import com.multicloud.batch.dao.huawei.payload.HuaweiResourceBillingRequest;
import com.multicloud.batch.dao.huawei.payload.HuaweiResourceBillingResponse;
import com.multicloud.batch.job.CustomDateRange;
import com.multicloud.batch.model.HuaweiBillingDailyCost;
import com.multicloud.batch.repository.HuaweiBillingDailyCostRepository;
import jakarta.persistence.EntityManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim Molla
 * Email: shamim.molla@vivasoftltd.com
 */

@Slf4j
@Service
@RequiredArgsConstructor
public class HuaweiBillingServiceImpl implements HuaweiBillingService {

    private final EntityManager entityManager;

    private final RestTemplate restTemplate;
    private final HuaweiBillingDailyCostRepository huaweiBillingDailyCostRepository;

    @Override
    public void fetchDailyServiceCostUsage(long organizationId, CustomDateRange range, String token) {

        Map<HuaweiBillingGroup, HuaweiBillingDailyCost> data = new HashMap<>();
        doRequest(range, token, 0, data);
        huaweiBillingDailyCostRepository.upsertHuaweiBillingDailyCosts(data.values(), entityManager);

        log.info("Huawei billing data fetched and stored successfully. Total results: {}", data.size());

    }

    private void doRequest(CustomDateRange range, String token, int offset, Map<HuaweiBillingGroup, HuaweiBillingDailyCost> data) {

        HuaweiResourceBillingRequest request = new HuaweiResourceBillingRequest(
                String.valueOf(range.year()),
                null,
                null,
                "false",
                "all",
                2,
                "DAILY",
                range.start().toString(),
                range.end().toString(),
                offset,
                1000
        );

        String url = "https://bss.myhuaweicloud.eu/v2/bills/customer-bills/res-records/query";

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.set("X-Auth-Token", token);
        headers.set("X-Language", "en_US");

        HttpEntity<HuaweiResourceBillingRequest> entity = new HttpEntity<>(request, headers);

        ResponseEntity<HuaweiResourceBillingResponse> response = restTemplate.exchange(
                url,
                HttpMethod.POST,
                entity,
                HuaweiResourceBillingResponse.class
        );

        if (response.getBody() != null) {

            response.getBody().monthly_records().forEach(row -> {

                HuaweiBillingGroup group = new HuaweiBillingGroup(
                        LocalDate.parse(row.bill_date()),
                        row.payer_account_id(),
                        row.customer_id(),
                        row.cloud_service_type(),
                        row.sku_code(),
                        row.resource_Type_code(),
                        row.region(),
                        row.charge_mode()
                );

                HuaweiBillingDailyCost cost = data.get(group);

                if (cost == null) {

                    HuaweiBillingDailyCost newCost = HuaweiBillingDailyCost.from(row, 1);
                    data.put(group, newCost);

                } else {

                    cost.setConsumeAmount(cost.getConsumeAmount().add(row.consume_amount()));
                    cost.setOfficialAmount(cost.getOfficialAmount().add(row.official_amount()));
                    cost.setDiscountAmount(cost.getDiscountAmount().add(row.discount_amount()));
                    cost.setCouponAmount(cost.getCouponAmount().add(row.coupon_amount()));

                    data.put(group, cost);
                }

            });

            if (!response.getBody().monthly_records().isEmpty()) {
                doRequest(range, token, offset + 1000, data);
            }

        } else {
            throw new RuntimeException("Failed to fetch daily service cost usage, response body is null");
        }

    }

}
