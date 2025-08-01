package com.multicloud.batch.dao.huawei;

import com.multicloud.batch.dao.huawei.payload.HuaweiResourceBillingRequest;
import com.multicloud.batch.dao.huawei.payload.HuaweiResourceBillingResponse;
import com.multicloud.batch.job.CustomDateRange;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClientResponseException;
import org.springframework.web.client.RestTemplate;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim Molla
 * Email: shamim.molla@vivasoftltd.com
 */

@Slf4j
@Service
@RequiredArgsConstructor
public class HuaweiBillingServiceImpl implements HuaweiBillingService {

    private final RestTemplate restTemplate;

    @Override
    public void fetchDailyServiceCostUsage(long organizationId, CustomDateRange range, String token) {

        doRequest(range, token, 0);

    }

    private void doRequest(CustomDateRange range, String token, int offset) {

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
//                "2025-07-28",
//                "2025-08-01",
                offset,
                1000
        );

        String url = "https://bss.myhuaweicloud.eu/v2/bills/customer-bills/res-records/query";

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.set("X-Auth-Token", token);
        headers.set("X-Language", "en_US");

        HttpEntity<HuaweiResourceBillingRequest> entity = new HttpEntity<>(request, headers);

        try {

            ResponseEntity<HuaweiResourceBillingResponse> response = restTemplate.exchange(
                    url,
                    HttpMethod.POST,
                    entity,
                    HuaweiResourceBillingResponse.class
            );

            if (response.getStatusCode() == HttpStatus.OK) {

                if (response.getBody() != null) {

//                response.getBody().monthly_records().forEach(System.out::println);

                    System.out.println(response.getBody().total_count());
                    System.out.println(response.getBody().monthly_records().size());

                    if (!response.getBody().monthly_records().isEmpty()) {
                        doRequest(range, token, offset + 1000);
                    }

                } else {
                    log.error("Response body is null");
                }

            } else {
                log.error("Error: {}", response.getStatusCode());
            }

        } catch (RestClientResponseException ex) {
            log.error("Error on fetching daily service cost usage", ex);
        }

    }

}
