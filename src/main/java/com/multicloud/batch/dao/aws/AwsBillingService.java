package com.multicloud.batch.dao.aws;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.Map;
import java.util.Set;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim Molla
 * Email: shamim.molla@vivasoftltd.com
 */

public interface AwsBillingService {

    Set<String> tableListByDatabase(String database, String accessKey, String secretKey, String region);

    Map<LocalDate, BigDecimal> getAzerionCostForExceptionalClients(String accessKey, String secretKey, String region);

    void syncDailyCostUsageFromAthena(
            String databaseName, String tableName, String accessKey, String secretKey, String region,
            LocalDate start, LocalDate end, boolean internal
    );

    void syncDailyCostUsageFromAthenaV2(
            String databaseName, String tableName, String accessKey, String secretKey, String region,
            LocalDate start, LocalDate end, boolean internal
    );

    void syncDailyCostUsageFromAthenaView(
            String databaseName, String tableName, String accessKey, String secretKey, String region,
            LocalDate start, LocalDate end
    );

}
