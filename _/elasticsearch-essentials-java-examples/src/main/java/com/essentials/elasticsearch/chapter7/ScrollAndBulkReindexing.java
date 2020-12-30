/**
 * @author bharvi
 */
package com.essentials.elasticsearch.chapter7;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import java.util.concurrent.TimeUnit;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHit;

import com.essentials.elasticsearch.connection.ES_Connection;

public class ScrollAndBulkReindexing  {

	//The maximum time to wait for the bulk requests to complete
    public static final int SCROLL_TIMEOUT_SECONDS = 30;
    //Number of documents to be returned, maximum would be scroll_size*number of shards
    public static final int SCROLL_SIZE = 10;
    //Sets when to flush a new bulk request based on the number of actions currently added defaults to 1000
    public static final int BULK_ACTIONS_THRESHOLD = 10000;
    //Sets the number of concurrent requests allowed to be executed.
    public static final int BULK_CONCURRENT_REQUESTS = 2;
    //Sets a flush interval flushing
    public static final int BULK_FLUSH_DURATION = 30;

    private Client clientFrom = ES_Connection.getEsConnection();
    private Client clientTo = ES_Connection.getEsConnection();

    
    public static void main(String[] args) {
		ScrollAndBulkReindexing object = new ScrollAndBulkReindexing();
		//call the execute method to start processing by passing actual source index and target index details
		object.execute("fromIndex_name", "sourceDocType", "toIndex", "destinationDocType");
	}
    /**
     * Method for fetching the data using scan-scoll and ad to the bulk processor for indexing
     * @param fromIndex
     * @param sourceDocType
     * @param toIndex
     * @param destinationDocType
     */
    public void execute(String fromIndex,String sourceDocType, String toIndex,String destinationDocType) {
        System.out.println("Start copying the data from"+fromIndex+" to "+toIndex);
        SearchResponse searchResponse = clientFrom.prepareSearch(fromIndex)
        		.setTypes(sourceDocType)
                .setQuery(matchAllQuery())
                .setSearchType(SearchType.SCAN)
                .setScroll(createScrollTimeoutValue())
                .setSize(SCROLL_SIZE).execute().actionGet();

        BulkProcessor bulkProcessor = BulkProcessor.builder(clientTo,
                createLoggingBulkProcessorListener()).setBulkActions(BULK_ACTIONS_THRESHOLD)
                .setConcurrentRequests(BULK_CONCURRENT_REQUESTS)
                .setFlushInterval(createFlushIntervalTime())
                .build();

        while (true) {
            searchResponse = clientFrom.prepareSearchScroll(searchResponse.getScrollId())
                    .setScroll(createScrollTimeoutValue()).execute().actionGet();
            if (searchResponse.getHits().getHits().length == 0) {
                System.out.println("Closing the bulk processor");
                bulkProcessor.close();
                break; //Break condition: No hits are returned
            }
            //Add the documents to the bulk processor and depending on the bulk threshold they will be flushed to ES
            for (SearchHit hit : searchResponse.getHits()) {
				IndexRequest request = new IndexRequest(toIndex, destinationDocType, hit.id());
                request.source(hit.getSource());
                bulkProcessor.add(request);
                }
        }
    }

    private BulkProcessor.Listener createLoggingBulkProcessorListener() {
        return new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
               System.out.println("Going to execute new bulk composed "+ request.numberOfActions()+" no. of actions");
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
            	System.out.println("Executed bulk composed "+ request.numberOfActions()+" no. of actions");
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                System.out.println("Error executing bulk "+ failure);
            }
        };
    }

    private TimeValue createFlushIntervalTime() {
        return new TimeValue(BULK_FLUSH_DURATION, TimeUnit.SECONDS);
    }

    private TimeValue createScrollTimeoutValue() {
        return new TimeValue(SCROLL_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }
}