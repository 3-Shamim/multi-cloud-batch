package com.multicloud.batch.dao.huawei;

import com.multicloud.batch.dao.huawei.payload.HuaweiAuthRequest;
import com.multicloud.batch.dao.huawei.payload.HuaweiAuthResponse;
import com.multicloud.batch.dao.huawei.payload.HuaweiAuthDetails;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.time.LocalDate;
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
public class HuaweiAuthServiceImpl implements HuaweiAuthService {

    private final Map<HuaweiAuthRequest, HuaweiAuthDetails> MAP = new ConcurrentHashMap<>();

    private final RestTemplate restTemplate;

    @Override
    public HuaweiAuthDetails login(String username, String password, String domain, String project) {

        HuaweiAuthRequest request = HuaweiAuthRequest.buildPasswordIdentity(
                username, password, domain, project
        );

        HuaweiAuthDetails cachedResponse = MAP.get(request);

        if (cachedResponse != null) {

            if (cachedResponse.date().isEqual(LocalDate.now())) {

                log.info("Serve token from cache: {}", cachedResponse.token());

                return cachedResponse;
            }

            MAP.remove(request);

        }

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        ResponseEntity<HuaweiAuthResponse> response = doAuthRequest(request, headers);

        // Get token from the header
        List<String> tokenHeader = response.getHeaders().get("X-Subject-Token");

        if (tokenHeader == null || tokenHeader.isEmpty() || response.getBody() == null) {
            throw new RuntimeException("Failed to get token from API response");
        }

        String payerAccountId = response.getBody().token().user().domain().id();

        HuaweiAuthDetails huaweiAuthDetails = new HuaweiAuthDetails(payerAccountId, tokenHeader.getFirst(), LocalDate.now());

        MAP.put(request, huaweiAuthDetails);

        log.info("Serve token from API request: {}", huaweiAuthDetails.token());

        return huaweiAuthDetails;
    }

    @Override
    public HuaweiAuthDetails getAssumeRoleToken(String token, String domainName, String agencyName, String project) {

        HuaweiAuthRequest request = HuaweiAuthRequest.buildAssumeRoleIdentity(domainName, agencyName, project);

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.set("X-Auth-Token", token);

        ResponseEntity<HuaweiAuthResponse> response = doAuthRequest(request, headers);

        // Get token from the header
        List<String> tokenHeader = response.getHeaders().get("X-Subject-Token");

        if (tokenHeader == null || tokenHeader.isEmpty() || response.getBody() == null) {
            throw new RuntimeException("Failed to get token from API response");
        }

        String payerAccountId = response.getBody().token().user().domain().id();

        return new HuaweiAuthDetails(payerAccountId, tokenHeader.getFirst(), LocalDate.now());
    }

    private ResponseEntity<HuaweiAuthResponse> doAuthRequest(HuaweiAuthRequest request, HttpHeaders headers) {

        String url = "https://iam.myhuaweicloud.eu/v3/auth/tokens?nocatalog=true";

        HttpEntity<HuaweiAuthRequest> entity = new HttpEntity<>(request, headers);

        return restTemplate.exchange(
                url,
                HttpMethod.POST,
                entity,
                HuaweiAuthResponse.class
        );
    }

}
