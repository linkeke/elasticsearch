package com.es.api;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import static org.elasticsearch.common.xcontent.XContentFactory.*;

import com.es.base.ESBase;

public class DocumentOperate extends ESBase{

	/**
	 * 获取JSON文档
	 * @param index 索引名
	 * @param type 类型
	 * @param id id值
	 * @return
	 */
	GetResponse getDocument(String index,String type,String id){
		GetResponse response = client.prepareGet(index, type, id).get();
		return response;
	}
	
	/**
	 * 删除JSON文档 
	 * @param index
	 * @param type
	 * @param id
	 * @return
	 */
	DeleteResponse deleteDocument(String index,String type,String id){
		DeleteResponse response = client.prepareDelete(index, type, id).get();
		return response;
	}
	
	/**
	 * 更新JSON文档中的数据
	 * @param index
	 * @param type
	 * @param id
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	void updateDocument(String index,String type,String id) throws IOException, InterruptedException, ExecutionException{
		UpdateRequest updateRequest = new UpdateRequest();
		updateRequest.index(index);
		updateRequest.type(type);
		updateRequest.id(id);
		updateRequest.doc(jsonBuilder()
		        .startObject()
		            .field("c_stage_name", "string")
		            .field("c_stage_name1", "string1")
		        .endObject());
		client.update(updateRequest).get();
	}
	
	/**
	 * 获取多个索引文档
	 * @return
	 */
	MultiGetResponse multiGetDocument(){
		
		MultiGetResponse multiGetItemResponses = client.prepareMultiGet()
			    .add("project", "owl_project", "")           			 
			    .add("project", "owl_project_finance", "")			    
			    .get();

			for (MultiGetItemResponse itemResponse : multiGetItemResponses) { 
			    GetResponse response = itemResponse.getResponse();
			    if (response.isExists()) {                      
			        String json = response.getSourceAsString(); 
			        System.out.println(json);
			    }
			}
		
		return null;
	}
	
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try {
			getClient("elasticsearch", "112.74.67.239", 9300);
			DocumentOperate aa = new DocumentOperate();
			aa.deleteDocument("project","owl_project","");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}

}
