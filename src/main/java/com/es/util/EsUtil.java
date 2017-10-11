package com.es.util;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;

public class EsUtil {
	
	protected Client client ; 
	
	protected String index ; 
	
	protected String type ; 
	
	protected String idKey ;
	
	/**
	 * 创建客户端
	 * @param clusterName
	 * @param host
	 * @param port
	 * @return
	 * @throws Exception
	 */
	public Client getClient(String clusterName,String host,Integer port) throws  Exception{
		Settings settings = Settings.settingsBuilder()
		        .put("cluster.name", clusterName).put("client.transport.sniff", true).build();
		Client client = TransportClient.builder().settings(settings).build()
		        .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), port));
		return client;
	}

	/**
	 * 创建Client客户端，并设置索引、类型、ID值
	 * @param clusterName
	 * @param host
	 * @param port
	 * @param index
	 * @param type
	 * @param idKey
	 * @return
	 */
	public EsUtil getElasticSearch(String clusterName
			 , String host , Integer port , String index , String type , String idKey){
		EsUtil search = new EsUtil();
		try {
			search.client = search.getClient(clusterName, host, port) ;
			search.idKey = idKey ;
			search.index = index ;
			search.type = type ; 
		} catch (Exception e) {
			throw new RuntimeException( e.getMessage() , e )  ;
		}		
		return search ; 
	}
		
	/**
	 * 获取client客户端
	 * @return
	 */
	public Client getClient() {
		return client;
	}
	
	/**
	 * 分页查询
	 * @param from
	 * @param size
	 * @param sortBuilder
	 * @param builders
	 * @return
	 */
	public SearchHits  query(int from, int size, SortBuilder sortBuilder, QueryBuilder[] builders){
				
		SearchRequestBuilder requestBuilder = client.prepareSearch(index).setTypes(type)
    	        .setSearchType(SearchType.DFS_QUERY_THEN_FETCH).setFrom(from).setSize(size);
		
		for(QueryBuilder builder:builders){
			requestBuilder.setQuery(builder);
		}
		if(null != sortBuilder){
			requestBuilder.addSort( sortBuilder );
		}
		
		SearchResponse actionGet = requestBuilder.setExplain(true).execute().actionGet();	
		SearchHits searchHits =  actionGet.getHits() ;		
		return searchHits;		
	}
	
	/**
	 * 根据项目名称关键字获取项目索引里的对应项目ID列表
	 * @param keywords
	 * @return
	 */
	public List<Long> getQueryProjectIds(String keywords){
		List<Long> ids = new ArrayList<Long>();
		EsUtil esUtil = new EsUtil();
		esUtil = esUtil.getElasticSearch("elasticsearch", "112.74.67.239", 9300, "project", "owl_project", "");
				
		QueryStringQueryBuilder builder = new QueryStringQueryBuilder(keywords).defaultField("c_project_name");;
		FieldSortBuilder sortBuilder = SortBuilders.fieldSort("n_hot_level").order(SortOrder.DESC);
		
		SearchHits searchHits = esUtil.query( 0 , 2000 , sortBuilder , new QueryBuilder[]{
				builder
		}) ;
		for (SearchHit hit : searchHits) {
			ids.add(Long.parseLong(hit.getId()));		
			 System.out.println( hit.getId()+":"+hit.getSource().get("c_project_name") +":" +hit.getSource().get("c_project_brief")+":" +hit.getSource().get("c_project_introduce") + ":"+hit.getSource().get("n_hot_level"));
		}
		return ids;
	}
	
	/**
	 * 根据关键字获取新闻索引里的对应新闻ID列表
	 * @param keywords
	 * @return
	 */
	public List<Long> getQueryNewsIds(String keywords){
		List<Long> ids = new ArrayList<Long>();
		EsUtil esUtil = new EsUtil();
		esUtil = esUtil.getElasticSearch("elasticsearch", "112.74.67.239", 9300, "news", "owl_news", "");
				
		QueryStringQueryBuilder builder = new QueryStringQueryBuilder(keywords).defaultField("c_news_title");;
		FieldSortBuilder sortBuilder = SortBuilders.fieldSort("c_news_title").order(SortOrder.ASC);
		
		SearchHits searchHits = esUtil.query( 0 , 2000 , sortBuilder , new QueryBuilder[]{
				builder
		}) ;
		for (SearchHit hit : searchHits) {
			ids.add(Long.parseLong(hit.getId()));	
			 System.out.println( hit.getId()+":"+hit.getSource().get("c_news_title") +":" +hit.getSource().get("c_news_content")+":" +hit.getSource().get("c_news_from") + ":"+hit.getSource().get("c_industry_name"));
		}
		return ids;
	}
	
	/**
	 * 根据关键字获取团队索引里的对应团队ID列表
	 * @param keywords
	 * @return
	 */
	public List<Long> getQueryTeamIds(String keywords){
		List<Long> ids = new ArrayList<Long>();
		EsUtil esUtil = new EsUtil();
		esUtil = esUtil.getElasticSearch("elasticsearch", "112.74.67.239", 9300, "team", "owl_team", "");
				
		QueryStringQueryBuilder builder = new QueryStringQueryBuilder(keywords).defaultField("c_team_name");;
		FieldSortBuilder sortBuilder = SortBuilders.fieldSort("c_team_name").order(SortOrder.ASC);
		
		SearchHits searchHits = esUtil.query( 0 , 2000 , sortBuilder , new QueryBuilder[]{
				builder
		}) ;
		for (SearchHit hit : searchHits) {
			ids.add(Long.parseLong(hit.getId()));	
			 System.out.println( hit.getId()+":"+hit.getSource().get("c_team_name") +":" +hit.getSource().get("c_team_position")+":" +hit.getSource().get("c_team_wechat") + ":"+hit.getSource().get("n_is_leader"));
		}
		return ids;
	}
	
	/**
	 * 根据公司关键字获取项目Ids
	 * @param keywords
	 * @return
	 */
	public List<Long> getQueryProjectIdsByComname(String keywords){
		List<Long> ids = new ArrayList<Long>();

		EsUtil esUtil = new EsUtil();
		esUtil = esUtil.getElasticSearch("elasticsearch", "112.74.67.239", 9300, "project", "owl_project", "");
				
		EsUtil esUtil1 = new EsUtil();
		esUtil1 = esUtil1.getElasticSearch("elasticsearch", "112.74.67.239", 9300, "project", "owl_project_license", "");
		QueryBuilder licenseqb = QueryBuilders.matchQuery("c_company_name", keywords);		
		System.out.println(licenseqb);		
		SearchHits searchHits = esUtil1.query( 0 , 2000 , null , new QueryBuilder[]{
				licenseqb
		}) ;
		for (SearchHit hit : searchHits) {					
			QueryBuilder projectqb = QueryBuilders.matchQuery("n_license_id", hit.getId());			
			SearchHits searchProHits = esUtil.query( 0 , 2000 , null , new QueryBuilder[]{
					projectqb
			}) ;			 
			for (SearchHit hitpro : searchProHits) {	
				ids.add(Long.parseLong(hitpro.getId()));	
				System.out.println( hitpro.getId()+":"+hitpro.getSource().get("c_project_name") +":" +hitpro.getSource().get("c_project_brief")+":" +hitpro.getSource().get("c_project_introduce"));
			}			
		}		
		return ids;		
	}
	
	/**
	 * 根据项目行业关键字获取项目Ids
	 * @param keywords
	 * @return
	 */
	public List<Long> getQueryProjectIdsByIndustry(String keywords){
		List<Long> ids = new ArrayList<Long>();
		
		EsUtil esUtil1 = new EsUtil();
		esUtil1 = esUtil1.getElasticSearch("elasticsearch", "112.74.67.239", 9300, "project", "owl_inudstry", "");
		QueryBuilder industryqb = QueryBuilders.matchQuery("c_industry_name", keywords);		
		System.out.println(industryqb);		
		SearchHits searchHits = esUtil1.query( 0 , 2000 , null , new QueryBuilder[]{
				industryqb
		}) ;
		
		EsUtil esUtil = new EsUtil();
		esUtil = esUtil.getElasticSearch("elasticsearch", "112.74.67.239", 9300, "project", "owl_project_industry", "");
		for (SearchHit hit : searchHits) {	
			
			System.out.println(hit.getId());
			
			QueryBuilder projectsqb = QueryBuilders.matchQuery("n_industry_id", hit.getId());			
			SearchHits searchProHits = esUtil.query( 0 , 2000 , null , new QueryBuilder[]{
					projectsqb
			}) ;			 
			for (SearchHit hitpro : searchProHits) {	
				ids.add(Long.parseLong(hitpro.getId()));	
				System.out.println( hitpro.getId()+":"+hitpro.getSource().get("c_project_name") +":" +hitpro.getSource().get("c_project_brief")+":" +hitpro.getSource().get("c_project_introduce"));
			}			
		}		
		return ids;		
	}
	
	
	
	public List<Long> getQuery(String keywords){
		List<Long> ids = new ArrayList<Long>();

		
		EsUtil esUtil = new EsUtil();
		esUtil = esUtil.getElasticSearch("elasticsearch", "112.74.67.239", 9300, "project", "owl_project", "");
				
		EsUtil esUtil1 = new EsUtil();
		esUtil1 = esUtil1.getElasticSearch("elasticsearch", "112.74.67.239", 9300, "project", "owl_project_license", "");
		QueryBuilder licenseqb = QueryBuilders.matchQuery("c_company_name", keywords);		
		System.out.println(licenseqb);		
		SearchHits searchHits = esUtil1.query( 0 , 2000 , null , new QueryBuilder[]{
				licenseqb
		}) ;
		for (SearchHit hit : searchHits) {					
			QueryBuilder projectqb = QueryBuilders.matchQuery("n_license_id", hit.getId());			
			SearchHits searchProHits = esUtil.query( 0 , 2000 , null , new QueryBuilder[]{
					projectqb
			}) ;			 
			for (SearchHit hitpro : searchProHits) {	
				ids.add(Long.parseLong(hitpro.getId()));	
				System.out.println( hitpro.getId()+":"+hitpro.getSource().get("c_project_name") +":" +hitpro.getSource().get("c_project_brief")+":" +hitpro.getSource().get("c_project_introduce"));
			}			
		}		
		return ids;		
	}
			
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		EsUtil esUtil = new EsUtil();
		
		String queryName = "互联网";
//		System.out.println(esUtil.getQueryProjectIds(queryName));
//		System.out.println(esUtil.getQueryNewsIds(queryName));
//		System.out.println(esUtil.getQueryTeamIds(queryName));
		
		System.out.println(esUtil.getQueryProjectIdsByIndustry(queryName));

	}
}
