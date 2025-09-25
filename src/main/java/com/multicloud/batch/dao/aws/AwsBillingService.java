package com.multicloud.batch.dao.aws;

import java.time.LocalDate;
import java.util.Set;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim Molla
 * Email: shamim.molla@vivasoftltd.com
 */

public interface AwsBillingService {

    Set<String> tableListByDatabase(String database, String accessKey, String secretKey, String region);

    void syncInternalProjectDailyCostUsageFromAthena(
            String databaseName, String tableName, String accessKey, String secretKey, String region,
            LocalDate start, LocalDate end
    );

}
