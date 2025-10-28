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
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDate;
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
                .reader(productReader())
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

                            billings.add(new BillingDTO(
                                    lastMonth,
                                    item.productId(),
                                    item.organizationId(),
                                    CloudProvider.valueOf(providerCost.cloudProvider()),
                                    providerCost.cost(),
                                    calculatePercentage(providerCost.cost(), pricing.handlingFee()),
                                    calculatePercentage(providerCost.cost(), pricing.supportFee()),
                                    invoiceNumber,
                                    LocalDate.now(),
                                    LocalDate.now().plusMonths(1)
                            ));

                        }

                    }

                    log.info("Writing {} billings to database", billings.size());

                    invoiceCostService.insert(billings);

                })
                .build();
    }

    @Bean
    public ItemReader<ProductDTO> productReader() {

        JdbcCursorItemReader<ProductDTO> reader = new JdbcCursorItemReader<>();
        reader.setDataSource(dataSource);
        reader.setSql("""
                    SELECT p.id, p.organization_id, o.internal
                    FROM products p
                        JOIN organizations o ON p.organization_id = o.id
                """);
        reader.setFetchSize(500);
        reader.setSaveState(false);
        reader.setVerifyCursorPosition(false);

        reader.setRowMapper((rs, rowNum) -> new ProductDTO(
                rs.getLong("id"),
                rs.getLong("organization_id"),
                rs.getBoolean("internal")
        ));

        try {
            reader.afterPropertiesSet();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return reader;
    }

    private BigDecimal calculatePercentage(BigDecimal cost, double percentage) {

        if (cost == null) {
            return BigDecimal.ZERO;
        }
        return cost.multiply(new BigDecimal(percentage)).divide(new BigDecimal(100), 2, RoundingMode.HALF_UP);
    }

}
