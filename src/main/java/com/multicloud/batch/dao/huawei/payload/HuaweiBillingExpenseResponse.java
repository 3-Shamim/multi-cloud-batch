package com.multicloud.batch.dao.huawei.payload;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.math.BigDecimal;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim Molla
 * Email: shamim.molla@vivasoftltd.com
 */

public record HuaweiBillingExpenseResponse(
        List<FeeRecord> fee_records,
        int total_count,
        String currency
) {

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record FeeRecord(
            String bill_date,
            int bill_type,
            String customer_id,
            String region,
            String region_name,
            String cloud_service_type,
            String resource_type,
            String cloud_service_type_name,
            String resource_type_name,
            String resource_id,
            String resource_name,
            String resource_tag,
            String product_id,
            String product_name,
            String product_spec_desc,
            String sku_code,
            String spec_size,
            String spec_size_measure_id,
            String trade_id,
            String id,
            String enterprise_project_id,
            String enterprise_project_name,
            String charge_mode,
            String order_id,
            String period_type,
            String usage_type,
            Double usage,
            String usage_measure_id,
            Double free_resource_usage,
            String free_resource_measure_id,
            Double ri_usage,
            String ri_usage_measure_id,
            BigDecimal unit_price,
            String unit,
            BigDecimal official_amount,
            BigDecimal discount_amount,
            BigDecimal amount,
            BigDecimal cash_amount,
            BigDecimal credit_amount,
            BigDecimal coupon_amount,
            BigDecimal flexipurchase_coupon_amount,
            BigDecimal stored_card_amount,
            BigDecimal bonus_amount,
            BigDecimal debt_amount,
            BigDecimal adjustment_amount,
            int measure_id,
            String relative_order_id
    ) {
    }
}

