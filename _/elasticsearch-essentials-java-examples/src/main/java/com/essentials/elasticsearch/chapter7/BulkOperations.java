/**
 * @author bharvi
 */

package com.essentials.elasticsearch.chapter7;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilders;

import com.essentials.elasticsearch.connection.ES_Connection;

public class BulkOperations {
	Client client = ES_Connection.getEsConnection();
	IndexRequestBuilder index;
	public static void main(String[] args) throws IOException {
		//Uncomment the below section of code to execute and test all the methods
		
		/*String indexName = "name_of_your_index";
		String docType = "name_of_your_doc_type";
		ArrayList<String > ids_to_fetch =new ArrayList<String>();
		ids_to_fetch.add("125");
		ids_to_fetch.add("123");
		
		BulkOperations operations = new BulkOperations();
		operations.createBulkIndexRequests(indexName, docType);
		operations.createBulkUpdateRequests(indexName, docType);
		operations.createBulkDeleteRequests(indexName, docType);
		operations.multiGetRequests(indexName, docType, ids_to_fetch);
		operations.multiSearchRequests(indexName, docType);
		*/

	}

	/**
	 * Method for creating and executing bulk index/create requests
	 * @param indexName
	 * @param docType
	 */
	private void createBulkIndexRequests(String indexName, String docType) {
		BulkRequestBuilder bulkRequests = client.prepareBulk();
		
		Map<String, Object> document1= new HashMap<String, Object>();
		Map<String, Object> document2= new HashMap<String, Object>();
		document1.put("screen_name", "d_bharvi");
		document1.put("followers_count", 2000);
		document1.put("create_at", "2015-09-20");

		document2.put("screen_name", "b44nz0r");
		document2.put("followers_count", 6000);
		document2.put("create_at", "2015-09-21");

		//Create individual index requests and add them into bulk request
		bulkRequests.add(new IndexRequest().index(indexName).type(docType).source(document1).opType("create").id("125"));
		bulkRequests.add(new IndexRequest().index(indexName).type(docType).source(document1).opType("index").id("123"));

		//Execute the bulk request
		try
		{
			BulkResponse bulkResponse =bulkRequests.execute().actionGet();

			if (bulkResponse.hasFailures())
			{
				//handle the failure scenarios
				for (BulkItemResponse bulkItemResponse : bulkResponse) {

				}
			}
		}
		catch (Exception e) {
			System.out.println(e.getCause());
			if(e.getCause().toString().contains("document already exists"))
			{
				System.out.println("Document already exist");
			}
			else {
				System.out.println("Please check elasticsearch server logs");
			}
		}
	}

	/**
	 * Method for creating and executing bulk update requests
	 * @param indexName
	 * @param docType
	 */
	private void createBulkUpdateRequests(String indexName, String docType) throws IOException {
		BulkRequestBuilder bulkRequests = client.prepareBulk();
		
		Map<String, Object> partialDoc1= new HashMap<String, Object>();
		Map<String, Object> partialDoc2= new HashMap<String, Object>();

		partialDoc1.put("user_name", "Bharvi Dixit");
		partialDoc2.put("user_name", "Harsimran Walia");

		//Create individual index requests and add them into bulk request
		bulkRequests.add(new UpdateRequest().index(indexName).type(docType).doc(partialDoc1).id("125"));
		bulkRequests.add(new UpdateRequest().index(indexName).type(docType).doc(partialDoc2).id("123"));

		//Execute the bulk request
		try
		{
			BulkResponse bulkResponse =bulkRequests.execute().actionGet();

			if (bulkResponse.hasFailures())
			{
				//handle the failure scenarios
				for (BulkItemResponse bulkItemResponse : bulkResponse) {

				}
			}
		}
		catch (Exception e) {
			if(e.getCause().toString().contains("document miss"))
			{
				System.out.println("Document already exist");
			}
			else {
				System.out.println("Please check elasticsearch server logs");
			}
		}

	}

	/**
	 * Method for creating and executing bulk delete requests
	 * @param indexName
	 * @param docType
	 */
	private void createBulkDeleteRequests(String indexName, String docType) {
		BulkRequestBuilder bulkRequests = client.prepareBulk();

		//Create individual delete requests and add them into bulk request
		bulkRequests.add(new DeleteRequest().index(indexName).type(docType).id("1252"));
		bulkRequests.add(new UpdateRequest().index(indexName).type(docType).id("123"));

		//Execute the bulk request
		BulkResponse bulkResponse =bulkRequests.execute().actionGet();

		if (bulkResponse.hasFailures())
		{
			//handle the failure scenarios
			for (BulkItemResponse bulkItemResponse : bulkResponse) {

			}
		}
	}

	/**
	 *  Method for showing example of multi get requests and executing bulk delete requests
	 * @param indexName
	 * @param docType
	 * @param ids_to_be_fetched : List of document ids to be fetched
	 */
	private void multiGetRequests(String indexName, String docType, ArrayList<String> ids_to_be_fetched) {
		MultiGetResponse responses = client.prepareMultiGet()
				.add(indexName, docType, ids_to_be_fetched)
				.execute().actionGet();
		
		for (MultiGetItemResponse itemResponse : responses) { 
			GetResponse response = itemResponse.getResponse();
			if (response.isExists()) {                      
				String json = response.getSourceAsString(); 
				System.out.println(json);
			}
		}
	}

	/**
	 * Method for showing example of multi-search request
	 * @param indexName
	 * @param docType
	 */
	private void multiSearchRequests(String indexName, String docType) {
		SearchRequestBuilder searchRequest1 = client.prepareSearch().setIndices(indexName).setTypes(docType)
				.setQuery(QueryBuilders.queryStringQuery("elasticsearch").defaultField("text")).setSize(1);
		SearchRequestBuilder searchRequest2 = client.prepareSearch().setIndices(indexName).setTypes(docType)
				.setQuery(QueryBuilders.matchQuery("screen_name", "d_bharvi")).setSize(1);

		MultiSearchResponse sr = client.prepareMultiSearch()
				.add(searchRequest1)
				.add(searchRequest1)
				.execute().actionGet();

		// You will get all individual responses from MultiSearchResponse
		long nbHits = 0;
		for (MultiSearchResponse.Item item : sr.getResponses()) {
			SearchResponse response = item.getResponse();
			nbHits += response.getHits().getTotalHits();
		}
	}
}
