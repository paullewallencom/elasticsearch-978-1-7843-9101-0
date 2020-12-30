/**
 * @author bharvi
 */

package com.essentials.elasticsearch.chapter4;

import java.io.IOException;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.metrics.MetricsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.avg.Avg;
import org.elasticsearch.search.aggregations.metrics.max.Max;
import org.elasticsearch.search.aggregations.metrics.min.Min;
import org.elasticsearch.search.aggregations.metrics.stats.Stats;
import org.elasticsearch.search.aggregations.metrics.stats.extended.ExtendedStats;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;

import com.essentials.elasticsearch.connection.ES_Connection;

public class MetricAggregationExamples {
	Client client = ES_Connection.getEsConnection();
	public static void main(String[] args) throws IOException {
		//Uncomment the below section of code to execute and test all the methods

//		String indexName = "name_of_your_index";
//		String docType = "name_of_your_doc_type";

	}

	/**
	 * Method for indexing a single document in a request
	 * @param indexName
	 * @param docType
	 */
	private void minAggregation(String indexName, String docType,String fieldName) {
		MetricsAggregationBuilder aggregation =
		        AggregationBuilders.min("agg").field(fieldName);
		SearchResponse response = client.prepareSearch(indexName).setTypes(docType)
				.setQuery(QueryBuilders.matchAllQuery()).addAggregation(aggregation)
				.execute().actionGet();
		Min agg = response.getAggregations().get("agg");
		double min = agg.getValue();
	}
	
	private void maxAggregation(String indexName, String docType,String fieldName) {
		MetricsAggregationBuilder aggregation =
		        AggregationBuilders.max("agg").field(fieldName);
		SearchResponse response = client.prepareSearch(indexName).setTypes(docType)
				.setQuery(QueryBuilders.matchAllQuery()).addAggregation(aggregation)
				.execute().actionGet();
		Max agg = response.getAggregations().get("agg");
		double max = agg.getValue();
	}
	
	private void sumAggregation(String indexName, String docType,String fieldName) {
		MetricsAggregationBuilder aggregation =
		        AggregationBuilders.sum("agg").field(fieldName);
		SearchResponse response = client.prepareSearch(indexName).setTypes(docType)
				.setQuery(QueryBuilders.matchAllQuery()).addAggregation(aggregation)
				.execute().actionGet();
		Sum agg = response.getAggregations().get("agg");
		double max = agg.getValue();
	}
	private void avgAggregation(String indexName, String docType,String fieldName) {
		MetricsAggregationBuilder aggregation =
		        AggregationBuilders.avg("agg").field(fieldName);
		SearchResponse response = client.prepareSearch(indexName).setTypes(docType)
				.setQuery(QueryBuilders.matchAllQuery()).addAggregation(aggregation)
				.execute().actionGet();
		Avg agg = response.getAggregations().get("agg");
		double max = agg.getValue();
	}
	private void statsAggregation(String indexName, String docType,String fieldName) {
		MetricsAggregationBuilder aggregation =
		        AggregationBuilders.stats("agg").field(fieldName);
		SearchResponse response = client.prepareSearch(indexName).setTypes(docType)
				.setQuery(QueryBuilders.matchAllQuery()).addAggregation(aggregation)
				.execute().actionGet();
		Stats agg = response.getAggregations().get("agg");
		double min = agg.getMin();
		double max = agg.getMax();
		double avg = agg.getAvg();
		double sum = agg.getSum();
		long count = agg.getCount();
	}
	private void extendedStatsAggregation(String indexName, String docType,String fieldName) {
		MetricsAggregationBuilder aggregation =
		        AggregationBuilders.extendedStats("agg").field(fieldName);
		SearchResponse response = client.prepareSearch(indexName).setTypes(docType)
				.setQuery(QueryBuilders.matchAllQuery()).addAggregation(aggregation)
				.execute().actionGet();
		ExtendedStats agg = response.getAggregations().get("agg");
		double min = agg.getMin();
		double max = agg.getMax();
		double avg = agg.getAvg();
		double sum = agg.getSum();
		long count = agg.getCount();
		double stdDeviation = agg.getStdDeviation();
		double sumOfSquares = agg.getSumOfSquares();
		double variance = agg.getVariance();
	}
}


