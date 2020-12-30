/**
 * @author bharvi
 */

package com.essentials.elasticsearch.chapter3;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService.ScriptType;
import org.elasticsearch.search.SearchHit;

import com.essentials.elasticsearch.connection.ES_Connection;

public class CrudOperations {
	Client client = ES_Connection.getEsConnection();
	public static void main(String[] args) throws IOException {
		//Uncomment the below section of code to execute and test all the methods

//		String indexName = "name_of_your_index";
//		String docType = "name_of_your_doc_type";
//
//		CrudOperations operations = new CrudOperations();
	
//		operations.indexDocument(indexName, docType);
//		operations.getDocument(indexName, docType);
//		operations.updateDocumentsingScript(indexName, docType);
//		operations.deleteDocument(indexName, docType);
//		operations.searchDocument(indexName, docType);


	}

	/**
	 * Method for indexing a single document in a request
	 * @param indexName
	 * @param docType
	 */
	private void indexDocument(String indexName, String docType) {

		Map<String, Object> document1= new HashMap<String, Object>();
		document1.put("screen_name", "d_bharvi");
		document1.put("followers_count", 2000);
		document1.put("create_at", "2015-09-20");
		IndexResponse response = client.prepareIndex().setIndex(indexName).setType(docType)
				.setSource(document1).setId("1").execute().actionGet();
		System.out.println(response);
	}

	/**
	 * Method for getting a single document in a request
	 * @param indexName
	 * @param docType
	 */
	private void getDocument(String indexName, String docType) {
		GetResponse response = client.prepareGet().setIndex(indexName).setType(docType)
				.setId("1").execute().actionGet();
	}

	/**
	 * Method for updating a single document in a request using doc parameter
	 * @param indexName
	 * @param docType
	 */
	private void updateDocumentsingDoc(String indexName, String docType) {
		Map<String, Object> partialDoc1= new HashMap<String, Object>();

		partialDoc1.put("user_name", "Bharvi Dixit");
		//Create individual index requests and add them into bulk request
		UpdateResponse response = client.prepareUpdate().setIndex(indexName).setType(docType)
				.setId("1").setDoc(partialDoc1).execute().actionGet();
	}

	/**
	 * Method for updating a single document in a request using inline script
	 * @param indexName
	 * @param docType
	 */
	private void updateDocumentsingScript(String indexName, String docType) {
		String script = "ctx._source.user_name = \"Alberto Paro\"";
		UpdateResponse response = client.prepareUpdate().setIndex(indexName).setType(docType)
				.setScript(new Script(script, ScriptType.INLINE, "groovy", null))
				.setId("1")
				.execute().actionGet();
		System.out.println(response);
	}
	/**
	 * Method for deleting a single document in a request
	 * @param indexName
	 * @param docType
	 */
	private void deleteDocument(String indexName, String docType) {
		DeleteResponse response = client.prepareDelete().setIndex(indexName).setType(docType)
				.setId("1").execute().actionGet();
	}

	/**
	 * Method for showing example of building queries and executing search request
	 * @param indexName
	 * @param docType
	 */
	private void searchDocument(String indexName, String docType) {
		QueryBuilder query =QueryBuilders.termQuery("screen_name", "d_bharvi");
		SearchResponse response = client.prepareSearch()
				.setIndices(indexName).setTypes(docType)
				.setQuery(query).setFrom(0).setSize(10)
				.execute().actionGet();
		
		SearchHit[] results = response.getHits().getHits();
		for (SearchHit hit : results) {
			System.out.println(hit.getSource());
			//process documents
		}

	}
}
