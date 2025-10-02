package com.multicloud.batch.dao.huawei;

import com.multicloud.batch.dao.huawei.payload.HuaweiAuthDetails;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim Molla
 * Email: shamim.molla@vivasoftltd.com
 */

public interface HuaweiAuthService {

    // It has an in-memory cache that stores the token till it expires. And serve it from the cache.
    // The token is automatically refreshed when its expiration time is less than 2 hours.
    HuaweiAuthDetails login(String username, String password, String domainName, String region);

    // It has an in-memory cache that stores the token till it expires. And serve it from the cache.
    // The token is automatically refreshed when its expiration time is less than 2 hours.
    HuaweiAuthDetails getAssumeRoleToken(String token, String domainName, String agencyName, String project);

}
