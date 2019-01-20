package com.elasticseach;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.http.HttpHost;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.DocWriteResponse.Result;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author siddaram
 *
 */
public class Application {

	// The config parameters for the connection
	private static final String HOST = "localhost";
	private static final int PORT_ONE = 9200;
	private static final String SCHEME = "http";

	private static RestHighLevelClient restHighLevelClient;
	private static ObjectMapper objectMapper = new ObjectMapper();
	private static final String INDEX = "attachments";
	private static final String TYPE = "documents";

	/**
	 * Implemented Singleton pattern here so that there is just one connection at a
	 * time.
	 * 
	 * @return RestHighLevelClient
	 */
	private static synchronized RestHighLevelClient makeConnection() {

		if (restHighLevelClient == null) {
			restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost(HOST, PORT_ONE, SCHEME)));
		}

		return restHighLevelClient;
	}

	/**
	 * @throws IOException
	 */
	private static synchronized void closeConnection() throws IOException {
		restHighLevelClient.close();
		restHighLevelClient = null;
	}

	/**
	 * @param path
	 * @return encoded file content string
	 * @throws IOException
	 */
	private static Map<String, Object> readFile(String path) throws IOException {
		Map<String, Object> dataMap = new HashMap<String, Object>();
		String encodedfile = null;
		FileInputStream fileInputStreamReader = null;
		File file = new File(path);
		try {
			fileInputStreamReader = new FileInputStream(file);
			byte[] bytes = new byte[(int) file.length()];
			fileInputStreamReader.read(bytes);
			encodedfile = new String(Base64.getEncoder().encodeToString(bytes));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} finally {
			if (fileInputStreamReader != null)
				fileInputStreamReader.close();
		}
		dataMap.put("file", encodedfile);
		return dataMap;
	}

	/**
	 * @param path
	 * @throws IOException
	 *             Insert Attachment
	 */
	private static void insertAttachment(String path,String id) throws IOException {

		Map<String, Object> dataMap = readFile(path);
		IndexRequest indexRequest = new IndexRequest(INDEX, TYPE, id).source(dataMap)
				.setPipeline("attachment");
		try {
			IndexResponse response = restHighLevelClient.index(indexRequest);
			Result result = response.getResult();
		} catch (ElasticsearchException e) {
			e.getDetailedMessage();
		} catch (java.io.IOException ex) {
			ex.getLocalizedMessage();
		}
	}

	/**
	 * @param content
	 * @return searchresponse Search Attachment by Content
	 */
	private static SearchResponse searchAttachments(String fieldName,String content) {
		SearchResponse getResponse = null;
		try {
			SearchRequest searchRequest = new SearchRequest(INDEX);
			SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
			MatchQueryBuilder builders = QueryBuilders.matchQuery(fieldName, content);
			searchSourceBuilder.query(builders);
			searchRequest.source(searchSourceBuilder);
			getResponse = restHighLevelClient.search(searchRequest);
		} catch (java.io.IOException e) {
			e.getLocalizedMessage();
		}
		return getResponse;
	}
	


	/**
	 * @param id
	 *            Delete attachment by id
	 */
	private static void deleteAttachmentById(String id) {
		DeleteRequest deleteRequest = new DeleteRequest(INDEX, TYPE, id);
		RefreshPolicy refreshPolicy = RefreshPolicy.IMMEDIATE;
		deleteRequest.setRefreshPolicy(refreshPolicy);
		try {
			DeleteResponse deleteResponse = restHighLevelClient.delete(deleteRequest);
			deleteResponse.forcedRefresh();
			System.out.println("deleted Id--> " + deleteResponse.getId());
		} catch (java.io.IOException e) {
			e.getLocalizedMessage();
		}
	}

	/**
	 * @param response
	 * @return List of Attachments
	 * @throws Exception
	 *             Read the search response
	 */
	private static List<Attachment> readResponse(SearchResponse response) throws Exception {
		List<Attachment> attachments = new ArrayList<Attachment>();
		if (response != null) {
			long totalCount = response.getHits().getTotalHits();
			if (totalCount >= 0) {
				SearchHit[] hits = response.getHits().getHits();
				for (SearchHit searchHit : hits) {
					String id = searchHit.getId();
					Attachment attachment = objectMapper.convertValue(searchHit.getSourceAsMap().get("attachment"),
							Attachment.class);
					attachment.setId(id);
					attachments.add(attachment);
				}
			} else {
				throw new Exception("Data not found");
			}
		} else {
			throw new Exception("Result not found!!");
		}
		return attachments;
	}

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		makeConnection();
		
		String filePath = "F:\\study\\elasticsearch-with-attachment-master\\llncs.pdf";
		String id=UUID.randomUUID().toString();
		
		System.out.println("Before calling insert attachment.........."+id);
		insertAttachment(filePath,id);
		System.out.println("After attachment insertion..........!!");
		
		SearchResponse response = searchAttachments("attachment.content","Making");
		//SearchResponse response = searchAttachments("_id",id);
		
		List<Attachment> attachments = readResponse(response);

		System.out.println("attachments--> " + attachments);
		for (Attachment attachment : attachments) {
			 deleteAttachmentById(attachment.getId());
		}
		closeConnection();
	}
}
