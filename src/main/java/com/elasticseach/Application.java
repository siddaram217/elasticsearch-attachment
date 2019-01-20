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
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Application {

    //The config parameters for the connection
    private static final String HOST = "localhost";
    private static final int PORT_ONE = 9200;
   // private static final int PORT_TWO = 9201;
    private static final String SCHEME = "http";

    private static RestHighLevelClient restHighLevelClient;
    private static ObjectMapper objectMapper = new ObjectMapper();

    private static final String INDEX = "attachments";
    private static final String TYPE = "documents";

    /**
     * Implemented Singleton pattern here
     * so that there is just one connection at a time.
     * @return RestHighLevelClient
     */
    private static synchronized RestHighLevelClient makeConnection() {

        if(restHighLevelClient == null) {
            restHighLevelClient = new RestHighLevelClient(
                    RestClient.builder(
                            new HttpHost(HOST, PORT_ONE, SCHEME)));
        }

        return restHighLevelClient;
    }

    private static synchronized void closeConnection() throws IOException {
        restHighLevelClient.close();
        restHighLevelClient = null;
    }

    
    private static String readFile(String path) throws IOException {
		
		String encodedfile = null;
		FileInputStream fileInputStreamReader=null;
		File file = new File(path);
		try {
			fileInputStreamReader = new FileInputStream(file);
		    byte[] bytes = new byte[(int) file.length()];
		    fileInputStreamReader.read(bytes);
		    encodedfile = new String(Base64.getEncoder().encodeToString(bytes));
		} catch (FileNotFoundException e) {
		    e.printStackTrace();
		}
		finally {
			if(fileInputStreamReader!=null)
				fileInputStreamReader.close();
		}
		return encodedfile;
	}
    
    private static void insertAttachment(String path) throws IOException{
        Map<String, Object> dataMap = new HashMap<String, Object>();
        
       String encodedString= readFile(path);
       dataMap.put("path", path);
       dataMap.put("file", encodedString);
       
       IndexRequest indexRequest = new IndexRequest(INDEX, TYPE, UUID.randomUUID().toString())
                .source(dataMap).setPipeline("attachment");
        try {
            IndexResponse response = restHighLevelClient.index(indexRequest);
            System.out.println("response "+response.toString());
           Result result = response.getResult();
           System.out.println(result);
        } catch(ElasticsearchException e) {
            e.getDetailedMessage();
        } catch (java.io.IOException ex){
            ex.getLocalizedMessage();
        }
    }
    
    private static SearchResponse searchAttachments(String content) {
        SearchResponse getResponse = null;
        try {
            SearchRequest searchRequest = new SearchRequest(INDEX);
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder(); 
            MatchQueryBuilder builders=QueryBuilders.matchQuery("attachment.content", content);
            searchSourceBuilder.query(builders); 
            searchRequest.source(searchSourceBuilder);
			getResponse = restHighLevelClient.search(searchRequest);
        } catch (java.io.IOException e){
            e.getLocalizedMessage();
        }
        return getResponse ;
    }

    private static Attachment updateAttachmentById(String id,String path){
        UpdateRequest updateRequest = new UpdateRequest(INDEX, TYPE, id)
                .fetchSource(true);    // Fetch Object after its update
        try {
        	String encodedString=readFile(path);
            updateRequest.doc(encodedString, XContentType.JSON);
            UpdateResponse updateResponse = restHighLevelClient.update(updateRequest);
            return objectMapper.convertValue(updateResponse.getGetResult().sourceAsMap(), Attachment.class);
        }catch (JsonProcessingException e){
            e.getMessage();
        } catch (java.io.IOException e){
            e.getLocalizedMessage();
        }
        System.out.println("Unable to update person");
        return null;
    }

    private static void deleteAttachmentById(String id) {
        DeleteRequest deleteRequest = new DeleteRequest(INDEX, TYPE, id);
        RefreshPolicy refreshPolicy=RefreshPolicy.IMMEDIATE;
		deleteRequest.setRefreshPolicy(refreshPolicy);
        try {
            DeleteResponse deleteResponse = restHighLevelClient.delete(deleteRequest);
            deleteResponse.forcedRefresh();
            System.out.println("deleted Id--> "+deleteResponse.getId());
        } catch (java.io.IOException e){
            e.getLocalizedMessage();
        }
    }
    
    private static List<Attachment> readResponse(SearchResponse response) throws Exception {
		List<Attachment> attachments=new ArrayList<Attachment>();
		if (response != null) {
			System.out.println("hits-->"+response.getHits());
			long totalCount = response.getHits().getTotalHits();
			System.out.println("totalCount--> "+totalCount);
			if (totalCount >= 0) {
				SearchHit[] hits = response.getHits().getHits();
				for (SearchHit searchHit : hits) {
					String id=searchHit.getId();
					 Attachment attachment = objectMapper.convertValue(searchHit.getSourceAsMap().get("attachment"),
							Attachment.class);
					attachment.setId(id);
					attachments.add(attachment);
				}
			}else {
				throw new Exception("Data not found");
			}
		} else {
			throw new Exception("Result not found!!");
		}
		return attachments;
	}

    public static void main(String[] args) throws Exception {

        makeConnection();
        String filePath ="C:\\Users\\siddaram\\Downloads\\college\\kea_NC420_2018_07_14_15_11_44.pdf"; 
		System.out.println("Before calling insert attachment..........!!");
        insertAttachment(filePath);
        System.out.println("After attachment insertion..........!!");
        
        System.out.println("Search Attachments..................!!");
        SearchResponse response = searchAttachments("Siddaganga");
        List<Attachment> attachments = readResponse(response);
        System.out.println("After Search Method..........!!");
        
        System.out.println("attachments--> "+attachments);
       for (Attachment attachment : attachments) {
        	 deleteAttachmentById(attachment.getId());
		}
        closeConnection();
    }
}
