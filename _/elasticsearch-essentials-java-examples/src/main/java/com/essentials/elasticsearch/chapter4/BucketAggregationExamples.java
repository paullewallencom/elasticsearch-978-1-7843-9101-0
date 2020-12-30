/**
 * @author bharvi
 */

package com.essentials.elasticsearch.chapter4;

import java.io.IOException;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.filter.Filter;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.range.Range;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.joda.time.DateTime;

import com.essentials.elasticsearch.connection.ES_Connection;

public class BucketAggregationExamples {
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

	private void termsAggregation(String indexName, String docType,String fieldName) {
		AggregationBuilder aggregation =
				AggregationBuilders.terms("agg").field(fieldName)
				.size(10);
		SearchResponse response = client.prepareSearch(indexName).setTypes(docType)
				.setQuery(QueryBuilders.matchAllQuery()).addAggregation(aggregation)
				.execute().actionGet();
		Terms screen_names = response.getAggregations().get("agg");

		// For each entry
		for (Terms.Bucket entry : screen_names.getBuckets()) {
			entry.getKey();      // Term
			entry.getDocCount(); // Doc count
		}
	}

	private void rangeAggregation(String indexName, String docType,String fieldName) {
		AggregationBuilder aggregation =
				AggregationBuilders
				.range("agg")
				.field(fieldName)
				.addUnboundedTo(1.0f)               // from -infinity to 1.0 (excluded)
				.addRange(1.0f, 100.0f)               // from 1.0 to 100.0 (excluded)
				.addUnboundedFrom(100.0f);            // from 100.0 to +infinity
		SearchResponse response = client.prepareSearch(indexName).setTypes(docType)
				.setQuery(QueryBuilders.matchAllQuery()).addAggregation(aggregation)
				.execute().actionGet();
		Range agg = response.getAggregations().get("agg");

		// For each entry
		for (Range.Bucket entry : agg.getBuckets()) {
			String key = entry.getKeyAsString();          	// Range as key
			Number from = (Number) entry.getFrom();      	// Bucket from
			Number to = (Number) entry.getTo();          // Bucket to
			long docCount = entry.getDocCount();    	// Doc count
		}
	}

	private void dateRangeAggregation(String indexName, String docType,String fieldName) {
		AggregationBuilder aggregation =
				AggregationBuilders
				.dateRange("agg")
				.field(fieldName)
				.format("yyyy")
				.addUnboundedTo("2000")    // from -infinity to 2000 (excluded)
				.addRange("2000", "2005")  // from 2000 to 2005 (excluded)
				.addUnboundedFrom("2005"); // from 2005 to +infinity
		SearchResponse response = client.prepareSearch(indexName).setTypes(docType)
				.setQuery(QueryBuilders.matchAllQuery()).addAggregation(aggregation)
				.execute().actionGet();

		Range agg = response.getAggregations().get("agg");

		// For each entry
		for (Range.Bucket entry : agg.getBuckets()) {
			String key = entry.getKeyAsString();                // Date range as key
			DateTime fromAsDate = (DateTime) entry.getFrom();   // Date bucket from as a Date
			DateTime toAsDate = (DateTime) entry.getTo();       // Date bucket to as a Date
			long docCount = entry.getDocCount();                // Doc count
		}

	}
	private void histogramAggregation(String indexName, String docType,String fieldName) {
		AggregationBuilder aggregation =
				AggregationBuilders
				.histogram("agg")
				.field(fieldName)
				.interval(5);
		SearchResponse response = client.prepareSearch(indexName).setTypes(docType)
				.setQuery(QueryBuilders.matchAllQuery()).addAggregation(aggregation)
				.execute().actionGet();
		Histogram agg = response.getAggregations().get("agg");
		for (Histogram.Bucket entry : agg.getBuckets()) {
			Long key = (Long) entry.getKey();       // Key
			long docCount = entry.getDocCount();    // Doc coun
		}
	}
	private void dateHistogramAggregation(String indexName, String docType,String fieldName) {
		AggregationBuilder aggregation =
				AggregationBuilders
				.dateHistogram("agg")
				.field(fieldName)
				.interval(DateHistogramInterval.YEAR);
				//DateHistogramInterval.days(10)
		SearchResponse response = client.prepareSearch(indexName).setTypes(docType)
				.setQuery(QueryBuilders.matchAllQuery()).addAggregation(aggregation)
				.execute().actionGet();
		Histogram agg = response.getAggregations().get("agg");
		for (Histogram.Bucket entry : agg.getBuckets()) {
			DateTime key = (DateTime) entry.getKey();    // Key
			String keyAsString = entry.getKeyAsString(); // Key as String
			long docCount = entry.getDocCount();         // Doc count
		}

	}
	private void filterAggregation(String indexName, String docType,String fieldName) {
		AggregationBuilder aggregation = 
				AggregationBuilders
				.filter("agg")
				.filter(QueryBuilders.termQuery("gender", "male"));
		SearchResponse response = client.prepareSearch(indexName).setTypes(docType)
				.setQuery(QueryBuilders.matchAllQuery()).addAggregation(aggregation)
				.execute().actionGet();
		Filter agg = response.getAggregations().get("agg");
		agg.getDocCount(); // Doc count
	}
}
