package com.multicloud.batch.dao.aws.payload;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.*;

import java.io.Serial;
import java.io.Serializable;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim Molla
 * Email: shamim.molla@vivasoftltd.com
 */

@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
@ToString
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class SecretPayload implements Serializable {

    @Serial
    private static final long serialVersionUID = 789213789509213498L;

    // AWS
    private String accessKey;
    private String secretKey;

    // GCP
    private String jsonKey;

    // Huawei
    private String username;
    private String password;
    private String domainName;
    private String region;

}
