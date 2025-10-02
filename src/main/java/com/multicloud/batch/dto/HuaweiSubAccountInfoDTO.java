package com.multicloud.batch.dto;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim Molla
 * Email: shamim.molla@vivasoftltd.com
 */

public record HuaweiSubAccountInfoDTO(
        long organizationId,
        String domainName,
        String agencyName,
        String project
) {

    public String getUniqueName() {
        return String.format("%s_#_%s_#_%s", domainName, agencyName, project);
    }

    public static HuaweiSubAccountInfoDTO build(String uniqueName) {
        String[] parts = uniqueName.split("_#_");
        return new HuaweiSubAccountInfoDTO(
                0,
                parts[0],
                parts[1],
                parts[3]
        );
    }

}
