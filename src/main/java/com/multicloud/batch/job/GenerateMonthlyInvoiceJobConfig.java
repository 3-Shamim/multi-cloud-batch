package com.multicloud.batch.job;

import com.multicloud.batch.dto.BillingDTO;
import com.multicloud.batch.dto.CloudProviderCostDTO;
import com.multicloud.batch.dto.OrganizationPricingDTO;
import com.multicloud.batch.dto.ProductDTO;
import com.multicloud.batch.enums.CloudProvider;
import com.multicloud.batch.service.InvoiceCostService;
import com.multicloud.batch.service.OrganizationPricingService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Date;
import java.time.YearMonth;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim Molla
 * Email: shamim.molla@vivasoftltd.com
 */

@Slf4j
@Configuration
@RequiredArgsConstructor
@ConditionalOnExpression("${batch_job.monthly_invoice.enabled}")
public class GenerateMonthlyInvoiceJobConfig {

    private final DataSource dataSource;
    private final JdbcTemplate jdbcTemplate;

    private final JobRepository jobRepository;
    private final PlatformTransactionManager platformTransactionManager;

    private final OrganizationPricingService organizationPricingService;
    private final InvoiceCostService invoiceCostService;

    @Bean
    public Job generateMonthlyInvoiceJob() {

        return new JobBuilder("generateMonthlyInvoiceJob", jobRepository)
                .start(cacheAllPricingStep())
                .next(generateMonthlyInvoiceStep())
                .build();
    }

    @Bean
    public Step cacheAllPricingStep() {
        return new StepBuilder("cacheAllPricingStep", jobRepository)
                .tasklet((contribution, chunkContext) -> {

                    log.info("Caching all pricing data");
                    organizationPricingService.cacheAllActivePricing();

                    return RepeatStatus.FINISHED;
                }, platformTransactionManager)
                .build();
    }

    @Bean
    public Step generateMonthlyInvoiceStep() {

        return new StepBuilder("generateMonthlyInvoiceStep", jobRepository)
                .<ProductDTO, ProductDTO>chunk(1, platformTransactionManager)
                .reader(monthlyInvoiceProductReader())
                .processor(item -> item)
                .writer(chunk -> {

                    YearMonth lastMonth = YearMonth.now().minusMonths(1);

                    List<BillingDTO> billings = new ArrayList<>();

                    for (ProductDTO item : chunk.getItems()) {

                        List<CloudProviderCostDTO> providerCosts = invoiceCostService.findCloudProviderCosts(
                                item.productId(),
                                item.organizationId(),
                                item.isInternalOrg(),
                                lastMonth.atDay(1),
                                lastMonth.atEndOfMonth()
                        );

                        long invoiceNumber = invoiceCostService.getLatestInvoiceNumber(item.productId());

                        for (CloudProviderCostDTO providerCost : providerCosts) {

                            invoiceNumber++;

                            OrganizationPricingDTO pricing = organizationPricingService.getPricing(
                                    item.organizationId(), CloudProvider.valueOf(providerCost.cloudProvider())
                            );

                            double handlingFee =  0;
                            double supportFee = 0;

                            if (pricing != null) {
                                handlingFee = pricing.handlingFee();
                                supportFee = pricing.supportFee();
                            }

                            billings.add(new BillingDTO(
                                    lastMonth,
                                    item.productId(),
                                    item.organizationId(),
                                    CloudProvider.valueOf(providerCost.cloudProvider()),
                                    providerCost.cost(),
                                    calculatePercentage(providerCost.cost(), handlingFee),
                                    calculatePercentage(providerCost.cost(), supportFee),
                                    invoiceNumber,
                                    lastMonth.atDay(4),
                                    lastMonth.atDay(4).plusMonths(1)
                            ));

                        }

                    }

                    log.info("Writing {} billings to database", billings.size());

                    insertBillings(billings);

                })
                .build();
    }

    @Bean
    public ItemReader<ProductDTO> monthlyInvoiceProductReader() {

        JdbcCursorItemReader<ProductDTO> reader = new JdbcCursorItemReader<>();
        reader.setDataSource(dataSource);
        reader.setSql("""
                    SELECT p.id, p.name, o.id AS org_id, o.name AS org_name, o.internal, o.exceptional
                    FROM products p
                        JOIN organizations o ON p.organization_id = o.id
                """);
        reader.setFetchSize(500);
        reader.setSaveState(false);
        reader.setVerifyCursorPosition(false);

        reader.setRowMapper((rs, rowNum) -> new ProductDTO(
                rs.getLong("id"),
                rs.getString("name"),
                rs.getLong("org_id"),
                rs.getString("org_name"),
                rs.getBoolean("internal"),
                rs.getBoolean("exceptional")
        ));

        try {
            reader.afterPropertiesSet();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return reader;
    }

    private void insertBillings(List<BillingDTO> billings) {

        String query = """
                INSERT INTO billings(
                    month_date, product_id, organization_id, cloud_provider,
                    cost, handling_fee, support_fee, invoice_number, created_date, due_date
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                """;

        jdbcTemplate.batchUpdate(query, billings, 100, (ps, bill) -> {
            ps.setDate(1, Date.valueOf(bill.month().atDay(1)));
            ps.setLong(2, bill.productId());
            ps.setLong(3, bill.organizationId());
            ps.setString(4, bill.provider().name());
            ps.setBigDecimal(5, bill.cost());
            ps.setBigDecimal(6, bill.handlingFee());
            ps.setBigDecimal(7, bill.supportFee());
            ps.setLong(8, bill.invoiceNumber());
            ps.setDate(9, Date.valueOf(bill.createdDate()));
            ps.setDate(10, Date.valueOf(bill.dueDate()));
        });

    }

    private BigDecimal calculatePercentage(BigDecimal cost, double percentage) {

        if (cost == null) {
            return BigDecimal.ZERO;
        }
        return cost.multiply(new BigDecimal(percentage)).divide(new BigDecimal(100), 2, RoundingMode.HALF_UP);
    }

}
