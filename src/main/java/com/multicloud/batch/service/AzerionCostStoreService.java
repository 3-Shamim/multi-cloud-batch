package com.multicloud.batch.service;

import com.multicloud.batch.dao.aws.AwsBillingService;
import com.multicloud.batch.dao.aws.payload.SecretPayload;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.util.Pair;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by IntelliJ IDEA.
 * User: Md Shamim
 * Date: 1/14/26
 * Email: mdshamim723@gmail.com
 */

@Slf4j
@AllArgsConstructor
@Service
public class AzerionCostStoreService {

    private final Map<Pair<LocalDate, String>, BigDecimal> AZERION_COST_MAP = new ConcurrentHashMap<>();

    private final AwsBillingService awsBillingService;

    public void fetchAndStoreAzerionCost(SecretPayload secret) {

        Map<Pair<LocalDate, String>, BigDecimal> azerionCostMap = awsBillingService.getAzerionCostForExceptionalClients(
                secret.getAccessKey(), secret.getSecretKey(), secret.getRegion()
        );

        AZERION_COST_MAP.putAll(azerionCostMap);

    }

    public Map<Pair<LocalDate, String>, BigDecimal> getAzerionCostMap() {
        return AZERION_COST_MAP;
    }

    public BigDecimal getAzerionCost(LocalDate date, String account) {
        return AZERION_COST_MAP.getOrDefault(Pair.of(date, account), BigDecimal.ZERO);
    }

}
