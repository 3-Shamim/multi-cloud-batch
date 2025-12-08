package com.multicloud.batch.dto;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim Molla
 * Email: shamim.molla@vivasoftltd.com
 */

public record ProductDTO(
        long productId,
        String productName,
        long organizationId,
        String organizationName,
        boolean isInternalOrg,
        boolean isExceptionalOrg
) {
}
