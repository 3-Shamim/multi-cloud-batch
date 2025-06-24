package com.multicloud.batch.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim Molla
 * Email: shamim.molla@vivasoftltd.com
 */

@Getter
@AllArgsConstructor
public enum CloudProvider {

    AWS("AWS", "Amazon Web Services"),
    GCP("GCP", "Google Cloud Platform"),
    HWC("HWC", "Huawei Cloud");

    private final String name;
    private final String description;

}
