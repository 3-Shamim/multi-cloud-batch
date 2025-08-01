package com.multicloud.batch.dao.huawei;

import com.multicloud.batch.dao.huawei.payload.HuaweiAuthRequest;
import com.multicloud.batch.dao.huawei.payload.HuaweiAuthResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClientResponseException;
import org.springframework.web.client.RestTemplate;

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
@RequiredArgsConstructor
public class HuaweiAuthServiceImpl implements HuaweiAuthService {

    private final Map<HuaweiAuthRequest, HuaweiAuthResponse> MAP = new HashMap<>();

    private final RestTemplate restTemplate;

    @Override
    public String login() {


        if (true) {
            throw new RuntimeException("For testing purpose only");
        }

        // IAM user
        String username = "";
        String password = "";

        // Master account name
        // Which account IAM user belongs to
        String domain = "";

        // Scope or Region
        String region = "";

        HuaweiAuthRequest request = HuaweiAuthRequest.build(
                region, domain, username, password
        );

        HuaweiAuthResponse cachedResponse = MAP.get(request);

        if (cachedResponse != null) {

            if (cachedResponse.date().isEqual(LocalDate.now())) {

                log.info("Serve token from cache: {}", cachedResponse.token());

                return cachedResponse.token();
            }

            MAP.remove(request);

        }

        String url = "https://iam.myhuaweicloud.eu/v3/auth/tokens?nocatalog=true";

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        HttpEntity<HuaweiAuthRequest> entity = new HttpEntity<>(request, headers);

        try {

            ResponseEntity<String> response = restTemplate.exchange(
                    url,
                    HttpMethod.POST,
                    entity,
                    String.class
            );

            if (response.getStatusCode() == HttpStatus.CREATED) {

                // Get token from the header
                List<String> tokenHeader = response.getHeaders().get("X-Subject-Token");

                if (tokenHeader != null && !tokenHeader.isEmpty()) {

                    String token = tokenHeader.getFirst();
                    MAP.put(request, new HuaweiAuthResponse(token, LocalDate.now()));

                    log.info("Serve token from API request: {}", token);

                    return token;

                } else {
                    log.error("Token header missing in response");
                    return null;
                }

            } else {
                log.error("Failed to fetch token, status: {}", response.getStatusCode());
                return null;
            }


        } catch (RestClientResponseException ex) {
            log.error("Error on fetching daily service cost usage", ex);
            return null;
        }

    }

}
