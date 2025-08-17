package com.multicloud.batch.dao.huawei;

import com.multicloud.batch.dao.huawei.payload.HuaweiAuthDetails;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim Molla
 * Email: shamim.molla@vivasoftltd.com
 */

public interface HuaweiAuthService {

    HuaweiAuthDetails login(String username, String password, String domainName, String region);

}
