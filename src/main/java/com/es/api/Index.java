package com.es.api;

import java.io.IOException;
import java.util.Date;

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import com.es.base.ESBase;

public class Index extends ESBase{

	public void index() throws IOException{	
		
		//生成json内容
		XContentBuilder builder =
		        XContentFactory.jsonBuilder().startObject().field("user", "destiny1020")
		            .field("postDate", new Date()).field("message", "Try ES").endObject();

		System.out.println(builder.string());		

	    IndexResponse indexResponse =
	        client.prepareIndex(indexPro, typePro, "").setSource(builder).execute().actionGet();

	    if (indexResponse != null && indexResponse.isCreated()) {
	      System.out.println("Index has been created !");

	      // read report from response
	      System.out.println("Index name: " + indexResponse.getIndex());
	      System.out.println("Type name: " + indexResponse.getType());
	      System.out.println("ID(optional): " + indexResponse.getId());
	      System.out.println("Version: " + indexResponse.getVersion());
	    } else {
	      System.err.println("Index creation failed.");
	    }
	}
	
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try {
			getClient("elasticsearch", "112.74.67.239", 9300);
			
			Index index = new Index();
			index.index();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}
