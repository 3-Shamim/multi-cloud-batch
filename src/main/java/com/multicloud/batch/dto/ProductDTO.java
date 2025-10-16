package com.multicloud.batch.dto;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim Molla
 * Email: shamim.molla@vivasoftltd.com
 */

public record ProductDTO(
        long productId,
        long organizationId,
        boolean internalOrg
) {
}
