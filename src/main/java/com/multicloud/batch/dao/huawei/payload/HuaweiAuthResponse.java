package com.multicloud.batch.dao.huawei.payload;

import java.time.LocalDate;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim Molla
 * Email: shamim.molla@vivasoftltd.com
 */

public record HuaweiAuthResponse(String token, LocalDate date) {

}
