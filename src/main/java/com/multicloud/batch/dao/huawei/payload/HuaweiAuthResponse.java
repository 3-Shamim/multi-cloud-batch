package com.multicloud.batch.dao.huawei.payload;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim Molla
 * Email: shamim.molla@vivasoftltd.com
 */

@JsonIgnoreProperties(ignoreUnknown = true)
public record HuaweiAuthResponse(
        Token token
) {

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Token(
            User user
    ) {

        @JsonIgnoreProperties(ignoreUnknown = true)
        public record User(
                Domain domain,
                String id,
                String name
        ) {

            @JsonIgnoreProperties(ignoreUnknown = true)
            public record Domain(
                    String id,
                    String name
            ) {
            }

        }

    }

}
