package com.multicloud.batch.job;

import com.multicloud.batch.dto.PerDayCostDTO;
import com.multicloud.batch.dto.ProductDTO;
import com.multicloud.batch.secondary.model.AwsCustomerDailyCost;
import com.multicloud.batch.service.AwsCustomerCostService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.sql.Date;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim Molla
 * Email: shamim.molla@vivasoftltd.com
 */

@Slf4j
@Configuration
@ConditionalOnExpression("${batch_job.aws_customer_cost.enabled}")
public class CalculateAwsCustomerCostJobConfig {

    private final DataSource dataSource;

    private final JobRepository jobRepository;

    private final JdbcTemplate secondaryJdbcTemplate;
    private final PlatformTransactionManager secondaryTransactionManager;

    private final AwsCustomerCostService awsCustomerCostService;

    public CalculateAwsCustomerCostJobConfig(DataSource dataSource,
                                             @Qualifier(value = "secondaryJdbcTemplate")
                                             JdbcTemplate secondaryJdbcTemplate,
                                             @Qualifier(value = "secondaryTransactionManager")
                                             PlatformTransactionManager secondaryTransactionManager,
                                             JobRepository jobRepository,
                                             AwsCustomerCostService awsCustomerCostService) {

        this.dataSource = dataSource;
        this.secondaryJdbcTemplate = secondaryJdbcTemplate;
        this.jobRepository = jobRepository;
        this.secondaryTransactionManager = secondaryTransactionManager;
        this.awsCustomerCostService = awsCustomerCostService;
    }

    @Bean
    public Job calculateAwsCustomerCostJob() {

        return new JobBuilder("calculateAwsCustomerCostJob", jobRepository)
                .start(calculateAwsCustomerCostStep())
                .build();
    }

    @Bean
    public Step calculateAwsCustomerCostStep() {

        return new StepBuilder("calculateAwsCustomerCostStep", jobRepository)
                .<ProductDTO, ProductDTO>chunk(1, secondaryTransactionManager)
                .reader(awsCustomerCostProductReader())
                .writer(chunk -> {

                    LocalDate end = LocalDate.now();
                    LocalDate start = end.minusMonths(3).withDayOfMonth(1);

                    for (ProductDTO productDTO : chunk.getItems()) {

                        List<PerDayCostDTO> customerCostList = awsCustomerCostService.findPerDayCustomerCost(
                                productDTO.productId(),
                                productDTO.organizationId(),
                                productDTO.isInternalOrg(),
                                start,
                                end
                        );

                        List<AwsCustomerDailyCost> customerDailyCostList = new ArrayList<>();

                        if (productDTO.isExceptionalOrg()) {

                            for (PerDayCostDTO dto : customerCostList) {

                                customerDailyCostList.add(
                                        AwsCustomerDailyCost.builder()
                                                .day(dto.usageDate())
                                                .mcOrgId(productDTO.organizationId())
                                                .mcOrgName(productDTO.organizationName())
                                                .customerName(productDTO.productName())
                                                .azerionCost(dto.cost())
                                                .customerCost(
                                                        dto.cost().add(dto.handlingFee()).add(dto.supportFee())
                                                )
                                                .external(!productDTO.isInternalOrg())
                                                .build()
                                );

                            }

                        } else {

                            Map<LocalDate, BigDecimal> perDayMap = new HashMap<>();

                            List<PerDayCostDTO> perDayAzerionCost = awsCustomerCostService.findPerDayAzerionCost(
                                    productDTO.productId(),
                                    productDTO.organizationId(),
                                    start,
                                    end
                            );

                            perDayAzerionCost.forEach(perDay -> perDayMap.put(
                                    perDay.usageDate(), perDay.cost()
                            ));

                            for (PerDayCostDTO dto : customerCostList) {

                                customerDailyCostList.add(
                                        AwsCustomerDailyCost.builder()
                                                .day(dto.usageDate())
                                                .mcOrgId(productDTO.organizationId())
                                                .mcOrgName(productDTO.organizationName())
                                                .customerName(productDTO.productName())
                                                .azerionCost(perDayMap.get(dto.usageDate()))
                                                .customerCost(
                                                        dto.cost().add(dto.handlingFee()).add(dto.supportFee())
                                                )
                                                .external(!productDTO.isInternalOrg())
                                                .build()
                                );

                            }

                        }

                        insertCosts(customerDailyCostList);

                    }

                })
                .build();

    }

    @Bean
    public ItemReader<ProductDTO> awsCustomerCostProductReader() {

        JdbcCursorItemReader<ProductDTO> reader = new JdbcCursorItemReader<>();
        reader.setDataSource(dataSource);
        reader.setSql("""
                    SELECT p.id, p.name, o.id AS org_id, o.name AS org_name, o.internal, o.exceptional
                    FROM products p
                        JOIN organizations o ON p.organization_id = o.id
                    WHERE o.internal = false
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

    private void insertCosts(List<AwsCustomerDailyCost> costs) {

        String query = """
                INSERT INTO aws_customer_daily_cost(
                    day, mc_org_id, mc_org_name, customer_name, azerion_cost, customer_cost, external
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON DUPLICATE KEY UPDATE
                    azerion_cost = VALUES(azerion_cost),
                    customer_cost = VALUES(customer_cost);
                """;

        secondaryJdbcTemplate.batchUpdate(query, costs, 100, (ps, cost) -> {
            ps.setDate(1, Date.valueOf(cost.getDay()));
            ps.setLong(2, cost.getMcOrgId());
            ps.setString(3, cost.getMcOrgName());
            ps.setString(4, cost.getCustomerName());
            ps.setBigDecimal(5, cost.getAzerionCost());
            ps.setBigDecimal(6, cost.getCustomerCost());
            ps.setBoolean(7, cost.isExternal());
        });

    }

}
