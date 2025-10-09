package com.multicloud.batch.job;

import com.multicloud.batch.dto.BillingDTO;
import com.multicloud.batch.dto.CloudProviderCostDTO;
import com.multicloud.batch.dto.ProductDTO;
import com.multicloud.batch.enums.CloudProvider;
import com.multicloud.batch.service.InvoiceCostService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
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

    private final InvoiceCostService invoiceCostService;

    @Bean
    public Job generateMonthlyInvoiceJob() {

        return new JobBuilder("generateMonthlyInvoiceJob", jobRepository)
                .start(generateMonthlyInvoiceStep())
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
                                lastMonth.atDay(1),
                                lastMonth.atEndOfMonth()
                        );

                        for (CloudProviderCostDTO providerCost : providerCosts) {

                            billings.add(new BillingDTO(
                                    lastMonth,
                                    item.productId(),
                                    item.organizationId(),
                                    CloudProvider.valueOf(providerCost.cloudProvider()),
                                    providerCost.cost()
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
        reader.setSql("SELECT id, organization_id FROM products");
        reader.setFetchSize(0);
        reader.setSaveState(false);
        reader.setVerifyCursorPosition(false);

        reader.setRowMapper((rs, rowNum) -> new ProductDTO(
                rs.getLong("id"),
                rs.getLong("organization_id")
        ));

        try {
            reader.afterPropertiesSet();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return reader;
    }

}
