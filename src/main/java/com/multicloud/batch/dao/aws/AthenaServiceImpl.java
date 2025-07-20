package com.multicloud.batch.dao.aws;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.athena.model.*;
import software.amazon.awssdk.services.athena.paginators.GetQueryResultsIterable;

import java.time.Duration;
import java.util.Set;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim Molla
 * Email: shamim.molla@vivasoftltd.com
 */

@Slf4j
@RequiredArgsConstructor
@Service
public class AthenaServiceImpl implements AthenaService {

    private final AthenaClient athenaClient;

    @Override
    public String submitAthenaQuery(String query, String outputLocation, String database) {

        StartQueryExecutionRequest request = StartQueryExecutionRequest.builder()
                .queryString(query)
                .queryExecutionContext(
                        QueryExecutionContext.builder().database(database).build()
                )
                .resultConfiguration(
                        ResultConfiguration.builder().outputLocation(outputLocation).build()
                )
                .workGroup("primary")
                .build();

        StartQueryExecutionResponse response = athenaClient.startQueryExecution(request);

        log.info("Athena query submitted successfully with query execution ID: {}", response.queryExecutionId());

        return response.queryExecutionId();
    }

    @Override
    public void waitForQueryToComplete(String executionId) {

        GetQueryExecutionRequest request = GetQueryExecutionRequest.builder()
                .queryExecutionId(executionId)
                .build();

        final long maxWaitMs = Duration.ofMinutes(10).toMillis();
        final long pollIntervalMs = 1000;
        long startTime = System.currentTimeMillis();

        while (true) {

            try {

                GetQueryExecutionResponse response = athenaClient.getQueryExecution(request);
                QueryExecutionStatus status = response.queryExecution().status();
                QueryExecutionState state = status.state();

                switch (state) {
                    case SUCCEEDED:
                        log.info("Athena query succeeded: {}", executionId);
                        return;

                    case FAILED:
                    case CANCELLED:
                        throw new RuntimeException("Athena query failed: " + status.stateChangeReason());

                    default:
                        long elapsed = System.currentTimeMillis() - startTime;
                        if (elapsed > maxWaitMs) {
                            throw new RuntimeException("Athena query timed out after waiting 10 minutes");
                        }
                        Thread.sleep(pollIntervalMs);
                }

            } catch (AthenaException e) {

                if (isRetryableAthenaException(e)) {

                    log.warn("Temporary error from Athena: {}. Retrying...", e.awsErrorDetails().errorMessage());

                    try {
                        Thread.sleep(2000 + (long) (Math.random() * 1000)); // backoff + jitter
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("Interrupted while waiting for retry", ie);
                    }

                } else {
                    throw new RuntimeException("Non-retryable Athena error", e);
                }

            } catch (Exception ex) {
                throw new RuntimeException("Unexpected error during Athena query polling", ex);
            }

        }

    }

    @Override
    public void stopAthenaQuery(String executionId) {

        try {

            StopQueryExecutionRequest stopRequest = StopQueryExecutionRequest.builder()
                    .queryExecutionId(executionId)
                    .build();

            athenaClient.stopQueryExecution(stopRequest);

            GetQueryExecutionRequest getRequest = GetQueryExecutionRequest.builder()
                    .queryExecutionId(executionId)
                    .build();

            GetQueryExecutionResponse getResponse = athenaClient.getQueryExecution(getRequest);

            if (getResponse.queryExecution().status().state().equals(QueryExecutionState.CANCELLED)) {
                log.info("The Athena query has been cancelled!");
            }

        } catch (AthenaException e) {
            log.error("Error on cancelling query: [{}]", executionId, e);
        }

    }

    @Override
    public GetQueryResultsIterable fetchQueryResults(String executionId) {

        GetQueryResultsRequest request = GetQueryResultsRequest.builder()
                .queryExecutionId(executionId)
                .build();

        return athenaClient.getQueryResultsPaginator(request);
    }

    @Override
    public void printQueryResults(String executionId) {

        fetchQueryResults(executionId)
                .forEach(res -> res.resultSet().rows().forEach(row -> {

                    StringBuilder rowData = new StringBuilder();

                    for (Datum datum : row.data()) {
                        rowData.append(datum.varCharValue()).append(",");
                    }

                    rowData.deleteCharAt(rowData.length() - 1);
                    rowData.append("\n");

                    System.out.println(rowData);

                }));

    }

    @Override
    public String wrapQueryWithUnloadCsvGzip(String selectQuery, String outputLocation) {

        return String.format("""
        UNLOAD (
            %s
        )
        TO '%s'
        WITH (
            format = 'TEXTFILE',
            field_delimiter = ',',
            compression = 'GZIP'
        );
        """, selectQuery.trim(), outputLocation);
    }

    private boolean isRetryableAthenaException(AthenaException e) {

        String code = e.awsErrorDetails() != null ? e.awsErrorDetails().errorCode() : "";

        return Set.of(
                "ThrottlingException", "TooManyRequestsException", "RequestLimitExceeded", "ServiceUnavailableException"
        ).contains(code);
    }

}
