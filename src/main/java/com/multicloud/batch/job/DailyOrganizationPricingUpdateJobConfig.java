package com.multicloud.batch.job;

import com.multicloud.batch.dto.DailyOrganizationPricingDTO;
import com.multicloud.batch.enums.CloudProvider;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.sql.Date;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim Molla
 * Email: shamim.molla@vivasoftltd.com
 */

@Slf4j
@Configuration
@RequiredArgsConstructor
@ConditionalOnExpression("${batch_job.daily_org_pricing_update.enabled}")
public class DailyOrganizationPricingUpdateJobConfig {

    private static final int CHUNK = 500;

    private final DataSource dataSource;
    private final JdbcTemplate jdbcTemplate;

    private final JobRepository jobRepository;
    private final PlatformTransactionManager platformTransactionManager;

    @Bean
    public Job dailyOrganizationPricingUpdateJob() {
        return new JobBuilder("dailyOrganizationPricingUpdateJob", jobRepository)
                .start(dailyOrganizationPricingUpdateStep())
                .build();
    }

    @Bean
    public Step dailyOrganizationPricingUpdateStep() {
        return new StepBuilder("dailyOrganizationPricingUpdateStep", jobRepository)
                .<DailyOrganizationPricingDTO, DailyOrganizationPricingDTO>chunk(CHUNK, platformTransactionManager)
                .reader(generateOrganizationDailyPricing())
                .writer(this::upsertAll)
                .build();
    }

    @Bean
    public ItemReader<DailyOrganizationPricingDTO> generateOrganizationDailyPricing() {

        String sql = """
                    WITH RECURSIVE date_range AS (
                        SELECT MIN(start_date) AS usage_date, GREATEST(MAX(start_date), CURDATE()) AS max_date
                        FROM organization_pricing
                        UNION ALL
                        SELECT DATE_ADD(usage_date, INTERVAL 1 DAY), max_date
                        FROM date_range
                        WHERE usage_date < max_date
                    ),
                    pricing_with_next AS (
                        SELECT
                            op.organization_id,
                            op.cloud_provider,
                            op.start_date,
                            COALESCE(
                                LEAD(op.start_date) OVER (
                                    PARTITION BY op.organization_id, op.cloud_provider
                                    ORDER BY op.start_date
                                ),
                                GREATEST(MAX(op.start_date) OVER (), CURDATE()) + INTERVAL 1 DAY
                            ) AS next_start_date,
                            op.discount,
                            op.handling_fee,
                            op.support_fee
                        FROM organization_pricing op
                    )
                    SELECT dr.usage_date AS pricing_date,
                        p.organization_id,
                        p.cloud_provider,
                        COALESCE(p.discount, 0) AS discount,
                        COALESCE(p.handling_fee, 0) AS handling_fee,
                        COALESCE(p.support_fee, 0) AS support_fee
                    FROM date_range dr
                        JOIN pricing_with_next p ON dr.usage_date >= p.start_date AND dr.usage_date < p.next_start_date;
                """;

        JdbcCursorItemReader<DailyOrganizationPricingDTO> reader = new JdbcCursorItemReader<>();

        reader.setDataSource(dataSource);
        reader.setSql(sql);
        reader.setVerifyCursorPosition(false);
        reader.setSaveState(false);

        reader.setFetchSize(500);

        reader.setRowMapper((rs, rowNum) -> DailyOrganizationPricingDTO.builder()
                .pricingDate(rs.getDate("pricing_date").toLocalDate())
                .organizationId(rs.getLong("organization_id"))
                .cloudProvider(CloudProvider.valueOf(rs.getString("cloud_provider")))
                .discount(rs.getDouble("discount"))
                .handlingFee(rs.getDouble("handling_fee"))
                .supportFee(rs.getDouble("support_fee"))
                .build());

        try {
            reader.afterPropertiesSet();
        } catch (Exception e) {
            throw new IllegalStateException("Spring Batch configuration problem: ", e);
        }

        return reader;
    }

    public void upsertAll(Chunk<? extends DailyOrganizationPricingDTO> records) {

        if (records == null || records.isEmpty()) {
            return;
        }

        log.info("Upserting daily organization pricing's {} records...", records.size());

        String sql = """
                INSERT INTO daily_organization_pricing (
                    pricing_date, organization_id, cloud_provider, discount, handling_fee, support_fee
                )
                VALUES (?, ?, ?, ?, ?, ?)
                ON DUPLICATE KEY UPDATE
                    discount = VALUES(discount),
                    handling_fee = VALUES(handling_fee),
                    support_fee = VALUES(support_fee)
                """;

        jdbcTemplate.batchUpdate(sql, records.getItems(), records.size(),
                (ps, daily) -> {

                    ps.setDate(1, Date.valueOf(daily.getPricingDate()));
                    ps.setLong(2, daily.getOrganizationId());
                    ps.setString(3, daily.getCloudProvider().name());
                    ps.setDouble(4, daily.getDiscount());
                    ps.setDouble(5, daily.getHandlingFee());
                    ps.setDouble(6, daily.getSupportFee());

                });

    }

}
