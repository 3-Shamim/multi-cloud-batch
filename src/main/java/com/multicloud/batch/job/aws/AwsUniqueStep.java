package com.multicloud.batch.job.aws;

import com.multicloud.batch.job.CustomDateRange;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim Molla
 * Email: shamim.molla@vivasoftltd.com
 */

record AwsUniqueStep(String tableName, CustomDateRange range) {
}