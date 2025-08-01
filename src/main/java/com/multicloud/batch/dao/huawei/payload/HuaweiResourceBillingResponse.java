package com.multicloud.batch.dao.huawei.payload;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim Molla
 * Email: shamim.molla@vivasoftltd.com
 */

import java.math.BigDecimal;
import java.util.List;

public record HuaweiResourceBillingResponse(
        List<MonthlyRecord> monthly_records, int total_count, String currency
) {
    public record MonthlyRecord(
            String cycle,
            String bill_date,
            Integer bill_type,
            String customer_id,
            String region,
            String region_name,
            String cloud_service_type,
            String resource_Type_code,
            String cloud_service_type_name,
            String resource_type_name,
            String res_instance_id,
            String resource_name,
            String resource_tag,
            String sku_code,
            String enterprise_project_id,
            String enterprise_project_name,
            Integer charge_mode,
            BigDecimal consume_amount,
            BigDecimal cash_amount,
            BigDecimal credit_amount,
            BigDecimal coupon_amount,
            BigDecimal flexipurchase_coupon_amount,
            BigDecimal stored_card_amount,
            BigDecimal bonus_amount,
            BigDecimal debt_amount,
            BigDecimal adjustment_amount,
            BigDecimal official_amount,
            BigDecimal discount_amount,
            Integer measure_id,
            Integer period_type,
            String root_resource_id,
            String parent_resource_id,
            String trade_id,
            String id,
            String product_spec_desc,
            String sub_service_type_code,
            String sub_service_type_name,
            String sub_resource_type_code,
            String sub_resource_type_name,
            String sub_resource_id,
            String sub_resource_name,
            String pre_order_id,
            List<AzCodeInfo> az_code_infos,
            String payer_account_id,
            String effective_time,
            String expire_time,
            String consume_time,
            String be_id,
            String extend_params
    ) {
    }

    public record AzCodeInfo(String az_code) {
    }
}
