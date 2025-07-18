package com.multicloud.batch.dao.aws;

import software.amazon.awssdk.services.athena.paginators.GetQueryResultsIterable;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim Molla
 * Email: shamim.molla@vivasoftltd.com
 */

public interface AthenaService {

    String submitAthenaQuery(String query, String outputLocation, String database);

    void waitForQueryToComplete(String executionId);

    void stopAthenaQuery(String executionId);

    GetQueryResultsIterable fetchQueryResults(String executionId);

    void printQueryResults(String executionId);

    String wrapQueryWithUnloadCsvGzip(String selectQuery, String outputLocation);

}
