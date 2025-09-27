package com.multicloud.batch.dao.google;

import java.time.LocalDate;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim Molla
 * Email: shamim.molla@vivasoftltd.com
 */

public interface GoogleBillingService {

    void fetchDailyServiceCostUsage(byte[] jsonKey, LocalDate start, LocalDate end);

}
