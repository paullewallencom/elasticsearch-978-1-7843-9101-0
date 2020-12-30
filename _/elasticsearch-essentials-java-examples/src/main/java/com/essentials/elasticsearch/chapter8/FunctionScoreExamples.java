/**
 * @author bharvi
 */

package com.essentials.elasticsearch.chapter8;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.elasticsearch.index.query.functionscore.exp.ExponentialDecayFunctionBuilder;
import org.elasticsearch.index.query.functionscore.fieldvaluefactor.FieldValueFactorFunctionBuilder;
import org.elasticsearch.index.query.functionscore.gauss.GaussDecayFunctionBuilder;
import org.elasticsearch.index.query.functionscore.lin.LinearDecayFunctionBuilder;
import org.elasticsearch.index.query.functionscore.script.ScriptScoreFunctionBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService.ScriptType;

import com.essentials.elasticsearch.connection.ES_Connection;

public class FunctionScoreExamples {
	Client client = ES_Connection.getEsConnection();
	IndexRequestBuilder index;
	public static void main(String[] args) throws IOException {
		//Uncomment the below section of code to execute and test all the methods

		/*String indexName = "profiles";
		String docType = "candidate";
		FunctionScoreExamples operations = new FunctionScoreExamples();
		operations.weightFunction(indexName, docType);
		operations.fieldValueFactorFunction(indexName, docType);
		operations.scriptScoreFunction(indexName, docType);
		operations.expDecayFunction(indexName, docType);*/
	}

	/**
	 * Method for showing example of weight function score query
	 * @param indexName
	 * @param docType
	 */
	private void weightFunction(String indexName, String docType) {

		FunctionScoreQueryBuilder functionQuery = new FunctionScoreQueryBuilder(QueryBuilders.termQuery("skills", "java"))
		.add(QueryBuilders.termQuery("skills", "python"),
				ScoreFunctionBuilders.weightFactorFunction(2)).boostMode("replace");

		SearchResponse response = client.prepareSearch().setIndices(indexName)
				.setTypes(docType).setQuery(functionQuery)
				.execute().actionGet();
		System.out.println(response);
	}
	
	/**
	 * Method for showing example of field_value_factor function score query
	 * @param indexName
	 * @param docType
	 */
	private void fieldValueFactorFunction(String indexName, String docType) {

		FunctionScoreQueryBuilder functionQuery = new FunctionScoreQueryBuilder(QueryBuilders.termQuery("skills", "java"))
		.add(new FieldValueFactorFunctionBuilder("total_experience")).boostMode("multiply");

		SearchResponse response = client.prepareSearch().setIndices(indexName)
				.setTypes(docType).setQuery(functionQuery)
				.execute().actionGet();
		System.out.println(response);
	}
	
	/**
	 * Method for showing example of script_score function score query
	 * @param indexName
	 * @param docType
	 */
	private void scriptScoreFunction(String indexName, String docType) {
		//create script as string
		String script = "final_score=0; skill_array = doc['skills'].toArray(); "
				+ "counter=0; while(counter<skill_array.size())"
				+ "{for(skill in skill_array_provided)"
				+ "{if(skill_array[counter]==skill)"
				+ "{final_score = final_score+doc['total_experience'].value};};"
				+ "counter=counter+1;};return final_score";
		ArrayList<String> skills = new ArrayList<String>();
		skills.add("java");
		skills.add("python");
		
		//Create a map to contain params to be passed in the script function
		Map<String, Object> params = new HashMap<String, Object>();
		params.put("skill_array_provided",skills);
		FunctionScoreQueryBuilder functionQuery = new FunctionScoreQueryBuilder(QueryBuilders.termQuery("skills", "java"))
		.add(new ScriptScoreFunctionBuilder(new Script(script, ScriptType.INLINE, "groovy", params))).boostMode("replace");

		SearchResponse response = client.prepareSearch().setIndices(indexName)
				.setTypes(docType).setQuery(functionQuery)
				.execute().actionGet();
		System.out.println(response);
	}
	
	/**
	 * Method for showing example of decay function score query
	 * @param indexName
	 * @param docType
	 */
	private void expDecayFunction(String indexName, String docType) {
		Map<String, Object> origin = new HashMap<String, Object>();
		String scale = "100km";
		origin.put("lat", "28.66");
		origin.put("lon", "77.22");
		FunctionScoreQueryBuilder functionQuery = new FunctionScoreQueryBuilder()
		.add(new ExponentialDecayFunctionBuilder("geo_code",origin, scale)).boostMode("multiply");
		//For Linear Decay Function use below syntax
		//.add(new LinearDecayFunctionBuilder("geo_code",origin, scale)).boostMode("multiply");
		//For Gauss Decay Function use below syntax
		//.add(new GaussDecayFunctionBuilder("geo_code",origin, scale)).boostMode("multiply");
		SearchResponse response = client.prepareSearch().setIndices(indexName)
				.setTypes(docType).setQuery(functionQuery)
				.execute().actionGet();
		System.out.println(response);
	}

}
