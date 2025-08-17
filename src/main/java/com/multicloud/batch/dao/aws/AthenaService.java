package com.multicloud.batch.dao.aws;

import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.athena.paginators.GetQueryResultsIterable;

/**
 * Created by IntelliJ IDEA.
 * User: Md. Shamim Molla
 * Email: shamim.molla@vivasoftltd.com
 */

public interface AthenaService {

    String submitAthenaQuery(String query, String outputLocation, String database, AthenaClient athenaClient);

    void waitForQueryToComplete(String executionId, AthenaClient athenaClient);

    void stopAthenaQuery(String executionId, AthenaClient athenaClient);

    GetQueryResultsIterable fetchQueryResults(String executionId, AthenaClient athenaClient);

    void printQueryResults(String executionId, AthenaClient athenaClient);

    String wrapQueryWithUnloadCsvGzip(String selectQuery, String outputLocation);

}
