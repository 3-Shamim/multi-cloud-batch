package com.multicloud.batch.dao.huawei;

import com.multicloud.batch.dao.huawei.payload.HuaweiAuthDetails;
import com.multicloud.batch.dao.huawei.payload.HuaweiBillingGroup;
import com.multicloud.batch.dao.huawei.payload.HuaweiResourceBillingRequest;
import com.multicloud.batch.dao.huawei.payload.HuaweiResourceBillingResponse;
import com.multicloud.batch.job.CustomDateRange;
import com.multicloud.batch.model.HuaweiBillingDailyCost;
import com.multicloud.batch.repository.HuaweiBillingDailyCostRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.*;
import org.springframework.jdbc.core.JdbcTemplate;
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

    private final JdbcTemplate jdbcTemplate;
    private final RestTemplate restTemplate;

    private final HuaweiBillingDailyCostRepository huaweiBillingDailyCostRepository;

    @Override
    public void fetchDailyServiceCostUsage(CustomDateRange range, HuaweiAuthDetails authDetails, boolean internal) {

        Map<HuaweiBillingGroup, HuaweiBillingDailyCost> data = new HashMap<>();
        doRequest(range, authDetails, 0, data, internal);
        huaweiBillingDailyCostRepository.upsertHuaweiBillingDailyCosts(data.values(), jdbcTemplate, internal);

        log.info("Huawei billing data fetched and stored successfully. Total results: {}", data.size());

        data.clear();

    }

    private void doRequest(CustomDateRange range, HuaweiAuthDetails authDetails, int offset,
                           Map<HuaweiBillingGroup, HuaweiBillingDailyCost> data, boolean internal) {

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
        headers.set("X-Auth-Token", authDetails.token());
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
                        row.enterprise_project_id(),
                        row.cloud_service_type(),
                        row.sku_code(),
                        row.resource_Type_code(),
                        row.region(),
                        row.charge_mode(),
                        row.bill_type()
                );

                HuaweiBillingDailyCost cost = data.get(group);

                if (cost == null) {

                    HuaweiBillingDailyCost newCost = HuaweiBillingDailyCost.from(row, internal);
                    data.put(group, newCost);

                } else {

                    if (internal) {

                        if (row.consume_amount() != null) {
                            cost.setConsumeAmount(cost.getConsumeAmount().add(row.consume_amount()));
                        }
                        if (row.debt_amount() != null) {
                            cost.setDebtAmount(cost.getDebtAmount().add(row.debt_amount()));
                        }
                        if (row.official_amount() != null) {
                            cost.setOfficialAmount(cost.getOfficialAmount().add(row.official_amount()));
                        }

                    } else {

                        if (row.consume_amount() != null) {
                            cost.setExtConsumeAmount(cost.getExtConsumeAmount().add(row.consume_amount()));
                        }
                        if (row.debt_amount() != null) {
                            cost.setExtDebtAmount(cost.getExtDebtAmount().add(row.debt_amount()));
                        }
                        if (row.official_amount() != null) {
                            cost.setExtOfficialAmount(cost.getExtOfficialAmount().add(row.official_amount()));
                        }

                    }

                    data.put(group, cost);
                }

            });

            if (!response.getBody().monthly_records().isEmpty()) {
                doRequest(range, authDetails, offset + 1000, data, internal);
            }

        } else {
            throw new RuntimeException("Failed to fetch daily service cost usage, response body is null");
        }

    }

//    private void doRequest(long orgId, CustomDateRange range, HuaweiAuthDetails authDetails, int offset,
//                           Map<HuaweiBillingGroup, HuaweiBillingDailyCost> data) {
//
//        String baseUrl = "https://bss.myhuaweicloud.eu/v2/bills/customer-bills/res-fee-records";
//
//        URI uri = UriComponentsBuilder.fromUriString(baseUrl)
//                .queryParam("cycle", String.format("%d-%02d", range.year(), range.month()))
//                .queryParam("include_zero_record", "false")
//                .queryParam("statistic_type", "3")
//                .queryParam("bill_date_begin", range.start().toString())
//                .queryParam("bill_date_end", range.end().toString())
//                .queryParam("offset", offset)
//                .queryParam("limit", 1000)
//                .build()
//                .encode()
//                .toUri();
//
//        HttpHeaders headers = new HttpHeaders();
//        headers.setContentType(MediaType.APPLICATION_JSON);
//        headers.set("X-Auth-Token", authDetails.token());
//        headers.set("X-Language", "en_US");
//
//        HttpEntity<Void> entity = new HttpEntity<>(headers);
//
//        ResponseEntity<HuaweiBillingExpenseResponse> response = restTemplate.exchange(
//                uri,
//                HttpMethod.GET,
//                entity,
//                HuaweiBillingExpenseResponse.class
//        );
//
//        if (response.getBody() != null) {
//
//            response.getBody().fee_records().forEach(row -> {
//
//                HuaweiBillingGroup group = new HuaweiBillingGroup(
//                        LocalDate.parse(row.bill_date()),
//                        authDetails.payerAccountId(),
//                        row.customer_id(),
//                        row.cloud_service_type(),
//                        row.sku_code(),
//                        row.resource_type(),
//                        row.region(),
//                        row.charge_mode()
//                );
//
//                HuaweiBillingDailyCost cost = data.get(group);
//
//                if (cost == null) {
//
//                    HuaweiBillingDailyCost newCost = HuaweiBillingDailyCost.from(row, authDetails.payerAccountId(), orgId);
//                    data.put(group, newCost);
//
//                } else {
//
//                    cost.setConsumeAmount(cost.getConsumeAmount().add(row.amount()));
//                    cost.setOfficialAmount(cost.getOfficialAmount().add(row.official_amount()));
//                    cost.setDiscountAmount(cost.getDiscountAmount().add(row.discount_amount()));
//                    cost.setCouponAmount(cost.getCouponAmount().add(row.coupon_amount()));
//
//                    data.put(group, cost);
//                }
//
//            });
//
//            if (!response.getBody().fee_records().isEmpty()) {
//                doRequest(orgId, range, authDetails, offset + 1000, data);
//            }
//
//        } else {
//            throw new RuntimeException("Failed to fetch daily service cost usage, response body is null");
//        }
//
//    }

}
