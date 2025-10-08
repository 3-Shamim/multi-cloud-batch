package com.multicloud.batch.job.merge_provider_data;

import java.io.Serializable;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim Molla
 * Email: shamim.molla@vivasoftltd.com
 */

record AccountIds(List<String> accountIds) implements Serializable {
}
