package com.multicloud.batch.job.gcp;

import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim Molla
 * Email: shamim.molla@vivasoftltd.com
 */

@Component
public class BillingQueryBuilder {

    public List<String> buildMonthlyQueries(boolean isInitial) {

        List<String> queries = new ArrayList<>();
        LocalDate start = isInitial ? LocalDate.now().minusYears(1) : LocalDate.now().minusDays(7);
        LocalDate end = LocalDate.now();

        LocalDate current = start;

        if (isInitial) {
            current = start.withDayOfMonth(1);
        }

        while (current.isBefore(end)) {

            LocalDate next = current.plusMonths(1);

            String query = """
                        SELECT
                            DATE(usage_start_time) AS start_date,
                            billing_account_id AS account_id,
                            project.id AS project_id,
                            service.description AS service_name,
                            SUM(cost) AS cost_amount_usd,
                            ANY_VALUE(currency) AS currency
                        FROM
                            `azerion-billing.azerion_billing_eu.gcp_billing_export_v1_*`
                        WHERE
                            usage_start_time >= ':start_date' AND usage_start_time < ':end_date'
                        GROUP BY
                            start_date, account_id, project_id, service_name
                    """;

            query = query
                    .replace(":start_date", current.format(DateTimeFormatter.ISO_DATE))
                    .replace(":end_date", next.format(DateTimeFormatter.ISO_DATE));

            queries.add(query);

            current = next;

        }

        return queries;
    }

}