package com.multicloud.batch.dao.huawei.payload;

import java.time.Instant;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim Molla
 * Email: shamim.molla@vivasoftltd.com
 */

public record HuaweiAuthDetails(
        String payerAccountId,
        String token,
        Instant instant
) {

}
