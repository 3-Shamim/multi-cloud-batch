package com.multicloud.batch.dao.aws;

import java.time.LocalDate;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim Molla
 * Email: shamim.molla@vivasoftltd.com
 */

public interface AwsBillingService {

    void syncDailyCostUsageFromAthena(
            long organizationId, String accessKey, String secretKey, String region, LocalDate start, LocalDate end
    );

}
