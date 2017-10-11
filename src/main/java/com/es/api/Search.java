package com.es.api;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;

import com.es.base.ESBase;
import com.es.model.SearchModel;
import com.owl.common.log.LogTool;
import com.owl.common.util.Help;
public class Search extends ESBase{
	static LogTool log = LogTool.getInstance(Search.class);
	public static void initEsSearch(String clusterName,String ip,int port){
		try {
			log.debug("创建elasticSearch客户端。。");
			getClient(clusterName, ip, port);
			log.debug("创建elasticSearch客户端成功。。");
		} catch (Exception e) {
			log.error("创建elasticSearch客户端失败！！！！", e);
			// TODO Auto-generated catch block
		}	
	}
	/**
	 * 查询
	 * @param from
	 * @param size
	 * @param sortBuilder
	 * @param builders
	 * @return
	 */
	public static SearchHits query(String index,String type,int from, int size, SortBuilder sortBuilder, QueryBuilder[] builders){			
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
	 * 项目名称、公司名、人员名查询热点项目
	 * @param keywords
	 * @return
	 */
	public static SearchModel queryHotProjectIds(int pageNow, int pageSize,String keywords){
		List<Long> ids = new ArrayList<Long>();
		SearchModel newsSearchModel = new SearchModel();

			QueryBuilder qb = null;
			if(Help.isNotNull(keywords)){
				qb = QueryBuilders.boolQuery().must(QueryBuilders.multiMatchQuery(keywords, "c_project_name","c_company_name","c_team_name"))				         
				            .mustNot(QueryBuilders.termQuery("n_hot_level", "0"));
			}else{										
				QueryBuilder qb1 = QueryBuilders.matchAllQuery();		
//				QueryBuilder qb2 = QueryBuilders.existsQuery("c_team_scale");
				QueryBuilder qb3 = QueryBuilders.existsQuery("t_last_finance_time");		
				qb = QueryBuilders.boolQuery().should(qb1).must(qb3);				
			}
						
			Long start = System.currentTimeMillis();									
			FieldSortBuilder sortBuilder = SortBuilders.fieldSort("n_inner_score");				
			int from=(pageNow-1)*pageSize;
//			SearchHits searchHits = query("project","owl_project", from , pageSize , sortBuilder , new QueryBuilder[]{
//					qb	
//			}) ;
			
			
			log.debug(qb);
			
			SearchRequestBuilder requestBuilder = client.prepareSearch("project").setTypes("owl_project")
	    	        .setSearchType(SearchType.DFS_QUERY_THEN_FETCH).setFrom(from).setSize(pageSize);
				
			for(QueryBuilder builder:new QueryBuilder[]{
					qb	
			}){
				requestBuilder.setQuery(builder);
			}
					
			FieldSortBuilder sortBuilder1 = SortBuilders.fieldSort("n_hot_level").order(SortOrder.DESC);	
			requestBuilder.addSort( sortBuilder1 );	
//			requestBuilder.addSort( sortBuilder );	
		
			SearchResponse actionGet = requestBuilder.setExplain(true).execute().actionGet();	
			SearchHits searchHits =  actionGet.getHits() ;		
		
			Long end = System.currentTimeMillis();			
			log.debug("时间差："+(end - start));						
			for (SearchHit hit : searchHits) {
				ids.add(Long.parseLong(hit.getId()));		
				 log.debug( hit.getId()+"公司名:"+hit.getSource().get("c_company_name") + ",项目名:"+hit.getSource().get("c_project_name") + ",热度:"+hit.getSource().get("n_hot_level")+ ",团队成员:"+hit.getSource().get("c_team_name"));
			}						
			
			long totalHits = searchHits.getTotalHits();
			Long pageCount=1l;
			long size=Long.parseLong(""+pageSize);
			if(totalHits%size==0){
				pageCount=totalHits/size;
			}else{
				pageCount=(totalHits/size)+1;
			}	
			
			newsSearchModel.setIds(ids);
			newsSearchModel.setPageCount(pageCount);
			
		
		 log.debug(ids);
		return newsSearchModel;
	}
	
	/**
	 * 项目名称、公司名、人员名查询最新项目
	 * @param keywords
	 * @return
	 */
	public static SearchModel queryNewProjectIds(int pageNow, int pageSize,String keywords){
		List<Long> ids = new ArrayList<Long>();
		SearchModel newsSearchModel = new SearchModel();

			Long start = System.currentTimeMillis();					
			if(Help.isNotNull(keywords)){
				//项目名称和公司名关键字搜索
				QueryBuilder qb1 = QueryBuilders.multiMatchQuery(keywords, "c_project_name","c_company_name","c_team_name").boost(2);	
				QueryBuilder qb = QueryBuilders.boolQuery().must(qb1);						
				FieldSortBuilder sortBuilder = SortBuilders.fieldSort("c_project_name").order(SortOrder.DESC);	
				int from=(pageNow-1)*pageSize;
				SearchHits searchHits = query("project","owl_project", from , pageSize , null , new QueryBuilder[]{
						qb	
				}) ;
				Long end = System.currentTimeMillis();			
				log.debug("时间差："+(end - start));			
				log.debug(qb);	
				for (SearchHit hit : searchHits) {
					ids.add(Long.parseLong(hit.getId()));		
					log.debug( hit.getId()+":内部得分=="+hit.getSource().get("n_inner_score")+":公司名=="+hit.getSource().get("c_company_name")+":项目名=="+hit.getSource().get("c_project_name")+"团队人员:"+hit.getSource().get("c_team_name")+ ",t_last_finance_time:" +":"+hit.getSource().get("n_hot_level")+ ",t_last_finance_time:"+hit.getSource().get("t_last_finance_time"));
				}	
				long totalHits = searchHits.getTotalHits();
				Long pageCount=1l;
				long size=Long.parseLong(""+pageSize);
				if(totalHits%size==0){
					pageCount=totalHits/size;
				}else{
					pageCount=(totalHits/size)+1;
				}	
				newsSearchModel.setIds(ids);
				newsSearchModel.setPageCount(pageCount);
				newsSearchModel.setTotalHit(totalHits);
			}else{
				//项目名称和公司名关键字搜索				
				QueryBuilder qb1 = QueryBuilders.matchAllQuery();	
//				QueryBuilder qb2 = QueryBuilders.existsQuery("c_team_scale");		
				QueryBuilder qb3 = QueryBuilders.existsQuery("t_last_finance_time");		
				QueryBuilder qb = QueryBuilders.boolQuery().should(qb1).must(qb3);	
//				log.debug(qb);
		
//				FieldSortBuilder sortBuilder = SortBuilders.fieldSort("n_inner_score").order(SortOrder.DESC);	
				FieldSortBuilder sortBuilder = SortBuilders.fieldSort("t_create_time").order(SortOrder.DESC);	
				int from=(pageNow-1)*pageSize;
				SearchHits searchHits = query("project","owl_project", from , pageSize , sortBuilder , new QueryBuilder[]{
						qb	
				}) ;
				
				log.debug("查询语句："+(qb));			
				
				Long end = System.currentTimeMillis();			
				log.debug("时间差："+(end - start));			
				
				for (SearchHit hit : searchHits) {
					ids.add(Long.parseLong(hit.getId()));		
					log.debug( hit.getId()+":内部得分=="+hit.getSource().get("n_inner_score")+":公司名=="+hit.getSource().get("c_company_name")+":项目名=="+hit.getSource().get("c_project_name") +":"+hit.getSource().get("n_hot_level")+ ",t_last_finance_time:"+hit.getSource().get("t_last_finance_time"));
				}	
			
				long totalHits = searchHits.getTotalHits();
				Long pageCount=1l;
				long size=Long.parseLong(""+pageSize);
				if(totalHits%size==0){
					pageCount=totalHits/size;
				}else{
					pageCount=(totalHits/size)+1;
				}	
				newsSearchModel.setIds(ids);
				newsSearchModel.setPageCount(pageCount);
				newsSearchModel.setTotalHit(totalHits);
			}
	
		 log.debug(ids);
		return newsSearchModel;
	}
	
	/**
	 * 项目名称、公司名、人员名查询有融资事件的项目
	 * @param keywords
	 * @return
	 */
	public static SearchModel queryFinanceProjectIds(int pageNow, int pageSize,String keywords){
		List<Long> ids = new ArrayList<Long>();
		SearchModel newsSearchModel = new SearchModel();

			Long start = System.currentTimeMillis();			
			if(Help.isNotNull(keywords)){
				//项目名称和公司名关键字搜索						
				QueryBuilder qb1 = QueryBuilders.multiMatchQuery(keywords, "c_project_name","c_company_name","c_team_name");	
//				QueryBuilder qb1 = QueryBuilders.boolQuery().should(QueryBuilders.matchQuery("c_project_name", keywords).boost(3)).should(QueryBuilders.matchQuery("c_company_name", keywords).boost(2)).should(QueryBuilders.matchQuery("c_team_name", keywords).boost(1));
				QueryBuilder qb2 = QueryBuilders.existsQuery("t_last_finance_time");
				QueryBuilder qb = QueryBuilders.boolQuery().must(qb1).must(qb2);	
				
				FieldSortBuilder sortBuilder = SortBuilders.fieldSort("c_project_name").order(SortOrder.DESC);		
				int from=(pageNow-1)*pageSize;
				SearchHits searchHits = query("project","owl_project", from , pageSize , null , new QueryBuilder[]{
						qb	
				}) ;
				Long end = System.currentTimeMillis();			
				log.debug(qb);			
				
				for (SearchHit hit : searchHits) {
					ids.add(Long.parseLong(hit.getId()));		
					log.debug( hit.getId()+":公司名=="+hit.getSource().get("c_company_name")+":项目名=="+hit.getSource().get("c_project_name") +":行业=="+hit.getSource().get("n_industry_id")+":融资阶段ID=="+hit.getSource().get("n_stage_id")+":团队成员=="+hit.getSource().get("c_team_name")+ ",t_last_finance_time:"+hit.getSource().get("t_last_finance_time"));
				}											
				
				Long teamProIds = searchHits.getTotalHits();
				Long totalHits = searchHits.getTotalHits() + teamProIds;
				Long pageCount=1l;
				long size=Long.parseLong(""+pageSize);
				if(totalHits%size==0){
					pageCount=totalHits/size;
				}else{
					pageCount=(totalHits/size)+1;
				}	
				
				newsSearchModel.setIds(ids);
				newsSearchModel.setPageCount(pageCount);
			}else{

				//项目名称和公司名关键字搜索				
				QueryBuilder qb = QueryBuilders.matchAllQuery();
				QueryBuilder qb2 = QueryBuilders.existsQuery("t_last_finance_time");
				
				FieldSortBuilder sortBuilder = SortBuilders.fieldSort("c_project_name").order(SortOrder.DESC);	
				int from=(pageNow-1)*pageSize;
				SearchHits searchHits = query("project","owl_project", from , pageSize , null , new QueryBuilder[]{
						qb,qb2	
				}) ;
				Long end = System.currentTimeMillis();			
				log.debug("时间差："+(end - start));			
				
				for (SearchHit hit : searchHits) {
					ids.add(Long.parseLong(hit.getId()));		
					log.debug( hit.getId()+":公司名=="+hit.getSource().get("c_company_name")+":项目名=="+hit.getSource().get("c_project_name") +":行业=="+hit.getSource().get("n_industry_id")+":融资阶段ID=="+hit.getSource().get("n_stage_id")+":团队成员=="+hit.getSource().get("c_team_name")+ ",t_last_finance_time:"+hit.getSource().get("t_last_finance_time"));
				}											
							
				Long totalHits = searchHits.getTotalHits();
				Long pageCount=1l;
				long size=Long.parseLong(""+pageSize);
				if(totalHits%size==0){
					pageCount=totalHits/size;
				}else{
					pageCount=(totalHits/size)+1;
				}	
				
				newsSearchModel.setIds(ids);
				newsSearchModel.setPageCount(pageCount);
			}
	
		 log.debug(ids);
		return newsSearchModel;
	}
	
	

	/**
	 * （网页）项目名称、公司名、人员名查询有融资事件的项目
	 * @param keywords
	 * @return
	 */
	public static SearchModel queryFinanceProjectIdsInWeb(int pageNow, int pageSize,String keywords,String industryIds){
		List<Long> ids = new ArrayList<Long>();
		SearchModel newsSearchModel = new SearchModel();
			
			Long start = System.currentTimeMillis();	
			
			
			if(Help.isNull(industryIds)){


				//项目名称和公司名关键字搜索				
				QueryBuilder qb = QueryBuilders.matchAllQuery();
				QueryBuilder qb2 = QueryBuilders.existsQuery("t_last_finance_time");
				
				FieldSortBuilder sortBuilder = SortBuilders.fieldSort("t_last_finance_time").order(SortOrder.DESC);	
				int from=(pageNow-1)*pageSize;
				SearchHits searchHits = query("project","owl_project", from , pageSize , sortBuilder , new QueryBuilder[]{
						qb,qb2	
				}) ;
				Long end = System.currentTimeMillis();			
				log.debug("时间差："+(end - start));			
				
				for (SearchHit hit : searchHits) {
					ids.add(Long.parseLong(hit.getId()));		
					log.debug( hit.getId()+":公司名=="+hit.getSource().get("c_company_name")+":项目名=="+hit.getSource().get("c_project_name") +":行业=="+hit.getSource().get("n_industry_id")+":融资阶段ID=="+hit.getSource().get("n_stage_id")+":团队成员=="+hit.getSource().get("c_team_name")+ ",t_last_finance_time:"+hit.getSource().get("t_last_finance_time"));
				}											
							
				Long totalHits = searchHits.getTotalHits();
				Long pageCount=1l;
				long size=Long.parseLong(""+pageSize);
				if(totalHits%size==0){
					pageCount=totalHits/size;
				}else{
					pageCount=(totalHits/size)+1;
				}	
				
				newsSearchModel.setIds(ids);
				newsSearchModel.setPageCount(pageCount);
			
			}else{

				QueryBuilder qb = null;
				BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
				QueryBuilder qb2 = QueryBuilders.existsQuery("t_last_finance_time");
				if(Help.isNotNull(keywords)){
					//项目名称和公司名关键字搜索						
					QueryBuilder qb1 = QueryBuilders.multiMatchQuery(keywords, "c_project_name","c_company_name","c_team_name");	
//					QueryBuilder qb1 = QueryBuilders.boolQuery().should(QueryBuilders.matchQuery("c_project_name", keywords).boost(3)).should(QueryBuilders.matchQuery("c_company_name", keywords).boost(2)).should(QueryBuilders.matchQuery("c_team_name", keywords).boost(1));
					
					qb = QueryBuilders.boolQuery().must(qb1).must(qb2);	
				}
				if(Help.isNotNull(industryIds)){
					String[] idArr = industryIds.split("#");
					boolQuery=boolQuery.must(QueryBuilders.termsQuery("n_industry_id", idArr)).must(qb2);
					QueryBuilders.termsQuery("n_industry_id", idArr);
				}
				
		
				qb = QueryBuilders.filteredQuery(qb,boolQuery);
				
				FieldSortBuilder sortBuilder = SortBuilders.fieldSort("t_last_finance_time").order(SortOrder.DESC);		
				int from=(pageNow-1)*pageSize;
				SearchHits searchHits = query("project","owl_project", from , pageSize , sortBuilder , new QueryBuilder[]{
						qb	
				}) ;
				Long end = System.currentTimeMillis();			
				log.debug(qb);			
				
				for (SearchHit hit : searchHits) {
					ids.add(Long.parseLong(hit.getId()));		
					log.debug( hit.getId()+":公司名=="+hit.getSource().get("c_company_name")+":项目名=="+hit.getSource().get("c_project_name") +":行业=="+hit.getSource().get("n_industry_id")+":融资阶段ID=="+hit.getSource().get("n_stage_id")+":团队成员=="+hit.getSource().get("c_team_name")+ ",t_last_finance_time:"+hit.getSource().get("t_last_finance_time"));
				}											
				
				Long teamProIds = searchHits.getTotalHits();
				Long totalHits = searchHits.getTotalHits() + teamProIds;
				Long pageCount=1l;
				long size=Long.parseLong(""+pageSize);
				if(totalHits%size==0){
					pageCount=totalHits/size;
				}else{
					pageCount=(totalHits/size)+1;
				}	
				
				newsSearchModel.setIds(ids);
				newsSearchModel.setPageCount(pageCount);
			
			}
			
		
		 log.debug(ids);
		return newsSearchModel;
	}
	
	
	/**
	 * 根据标签查询竞品项目
	 * @param keywords
	 * @return
	 */
	public static SearchModel queryCompetingProjectIds(int pageNow, int pageSize,String keywords){
		List<Long> ids = new ArrayList<Long>();
		SearchModel newsSearchModel = new SearchModel();
			
			Long start = System.currentTimeMillis();			
			if(Help.isNotNull(keywords)){
				//项目名称和公司名关键字搜索
				QueryBuilder qb1 = QueryBuilders.multiMatchQuery(keywords, "c_tag_name");				
				QueryBuilder qb = QueryBuilders.boolQuery().must(qb1);	
				
				FieldSortBuilder sortBuilder = null;	
				int from=(pageNow-1)*pageSize;
				SearchHits searchHits = query("project","owl_project", from , pageSize , sortBuilder , new QueryBuilder[]{
						qb	
				}) ;
				Long end = System.currentTimeMillis();			
				log.debug(qb);			
				
				for (SearchHit hit : searchHits) {
					ids.add(Long.parseLong(hit.getId()));		
					log.debug( hit.getId()+":"+hit.getSource().get("c_company_name") +":" +hit.getSource().get("c_project_brief")+":" +hit.getSource().get("c_project_introduce") + ":"+hit.getSource().get("n_hot_level")+ ",t_last_finance_time:"+hit.getSource().get("t_last_finance_time"));
				}											
				
				Long teamProIds = searchHits.getTotalHits();
				Long totalHits = searchHits.getTotalHits() + teamProIds;
				Long pageCount=1l;
				long size=Long.parseLong(""+pageSize);
				if(totalHits%size==0){
					pageCount=totalHits/size;
				}else{
					pageCount=(totalHits/size)+1;
				}	
				
				newsSearchModel.setIds(ids);
				newsSearchModel.setPageCount(pageCount);
			}else{

				//项目名称和公司名关键字搜索				
				QueryBuilder qb = QueryBuilders.matchAllQuery();
				QueryBuilder qb2 = QueryBuilders.existsQuery("t_last_finance_time");
				
				FieldSortBuilder sortBuilder = SortBuilders.fieldSort("t_last_finance_time").order(SortOrder.DESC);	
				int from=(pageNow-1)*pageSize;
				SearchHits searchHits = query("project","owl_project", from , pageSize , null , new QueryBuilder[]{
						qb,qb2	
				}) ;
				Long end = System.currentTimeMillis();			
				log.debug("时间差："+(end - start));			
				
				for (SearchHit hit : searchHits) {
					ids.add(Long.parseLong(hit.getId()));		
					log.debug( hit.getId()+":"+hit.getSource().get("c_project_name") +":" +hit.getSource().get("c_project_brief")+":" +hit.getSource().get("c_project_introduce") + ":"+hit.getSource().get("n_hot_level")+ ",t_last_finance_time:"+hit.getSource().get("t_last_finance_time"));
				}											
							
				Long totalHits = searchHits.getTotalHits();
				Long pageCount=1l;
				long size=Long.parseLong(""+pageSize);
				if(totalHits%size==0){
					pageCount=totalHits/size;
				}else{
					pageCount=(totalHits/size)+1;
				}	
				
				newsSearchModel.setIds(ids);
				newsSearchModel.setPageCount(pageCount);
			}
			
				
		 log.debug(ids);
		return newsSearchModel;
	}
	
	/**
	 * 根据新闻标题关键字搜索新闻ID列表
	 * @param keywords
	 * @return
	 */
	public static SearchModel queryNewsIds(int pageNow, int pageSize,String keywords){
		SearchModel newsSearchModel = new SearchModel();
		List<Long> ids = new ArrayList<Long>();

			QueryBuilder qb = null;
			FieldSortBuilder sortBuilder = null;
			if(Help.isNotNull(keywords)){			
				qb = QueryBuilders.boolQuery().must(QueryBuilders.termQuery("c_news_title", keywords));				
				sortBuilder = SortBuilders.fieldSort("t_news_time").order(SortOrder.DESC);
			}else{
				qb = QueryBuilders.matchAllQuery();
				sortBuilder = SortBuilders.fieldSort("t_news_time").order(SortOrder.DESC);
			}
			
			//新闻标题、新闻内容关键字搜索
			log.debug(qb);					
			Long start = System.currentTimeMillis();				
			int from=(pageNow-1)*pageSize;
			SearchHits searchHits = query("news","owl_news", from , pageSize , sortBuilder , new QueryBuilder[]{
					qb	
			}) ;
			Long end = System.currentTimeMillis();			
			log.debug("时间差："+(end - start));		
			log.debug("多少条："+searchHits.getTotalHits());
			long totalHits = searchHits.getTotalHits();
			Long pageCount=1l;
			long size=Long.parseLong(""+pageSize);
			if(totalHits%size==0){
				pageCount=totalHits/size;
			}else{
				pageCount=(totalHits/size)+1;
			}	
			for (SearchHit hit : searchHits) {
				ids.add(Long.parseLong(hit.getId()));		
				log.debug( hit.getId()+":"+hit.getSource().get("c_news_title")+":"+hit.getSource().get("t_news_time"));
			}	
			
			newsSearchModel.setIds(ids);
			newsSearchModel.setPageCount(pageCount);
		
		 log.debug(ids);
		 return newsSearchModel;
	}
	
	
	/**
	 * 项目名称、公司名、阶段ids、领域ids查询项目
	 * @param keywords
	 * @return
	 */
	public static SearchModel queryProjectIds(int pageNow, int pageSize,String keywords,String industryIds,String stageIds,String provinceIds,String cityIds ){
		List<Long> ids = new ArrayList<Long>();
		SearchModel newsSearchModel = new SearchModel();
	
			Long start = System.currentTimeMillis();			
			//根据in关键字搜索
			int from=(pageNow-1)*pageSize;		
						
			QueryBuilder kerwordBuilder = null;					
			QueryBuilder qb = null;
//			FieldSortBuilder sortBuilder = SortBuilders.fieldSort("c_project_name").order(SortOrder.DESC);	
			
			SearchRequestBuilder requestBuilder = null;
			
			BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
			if(Help.isNull(keywords)&&Help.isNull(industryIds)&&Help.isNull(stageIds)&&Help.isNull(cityIds)){
				
				FieldSortBuilder sortBuilder = SortBuilders.fieldSort("t_create_time").order(SortOrder.DESC);
				kerwordBuilder = QueryBuilders.boolQuery().must(QueryBuilders.matchAllQuery()).must(QueryBuilders.existsQuery("t_last_finance_time"));
				
				qb = QueryBuilders.filteredQuery(kerwordBuilder,boolQuery);	
				log.debug(qb);			
				requestBuilder = client.prepareSearch("project").setTypes("owl_project")
		    	        .setSearchType(SearchType.DFS_QUERY_THEN_FETCH).setFrom(from).setSize(pageSize);		
				requestBuilder.setQuery(qb);
				requestBuilder.addSort(sortBuilder);
				
			}else{

				FieldSortBuilder sortBuilder = null;
				if(Help.isNotNull(keywords)&&Help.isNull(industryIds)&&Help.isNull(stageIds)&&Help.isNull(provinceIds)&&Help.isNull(cityIds)){
					kerwordBuilder = QueryBuilders.boolQuery().must(QueryBuilders.multiMatchQuery(keywords, "c_project_name","c_company_name","c_team_name"));
					qb = QueryBuilders.filteredQuery(kerwordBuilder,boolQuery);	
					log.debug(qb);	
					requestBuilder = client.prepareSearch("project").setTypes("owl_project")
			    	        .setSearchType(SearchType.DFS_QUERY_THEN_FETCH).setFrom(from).setSize(pageSize);
					requestBuilder.setQuery(qb);					
				}else{
					if(Help.isNotNull(keywords)){
						kerwordBuilder = QueryBuilders.boolQuery().must(QueryBuilders.multiMatchQuery(keywords, "c_project_name","c_company_name","c_team_name"));
					}
					if(Help.isNotNull(industryIds)){
						String[] idArr = industryIds.split("#");
						boolQuery=boolQuery.must(QueryBuilders.termsQuery("n_industry_id", idArr));
						QueryBuilders.termsQuery("n_industry_id", idArr);
					}
					
					if(Help.isNotNull(stageIds)){		
						String[] idArr = stageIds.split("#");
						boolQuery=boolQuery.must(QueryBuilders.termsQuery("n_stage_id", idArr));
					}
					
					if(Help.isNotNull(provinceIds)){		
						String[] provinceIdsArr = provinceIds.split("#");
						boolQuery=boolQuery.must(QueryBuilders.termsQuery("n_province_id", provinceIdsArr));
					}
					
					if(Help.isNotNull(cityIds)){		
						String[] provinceIdsArr = cityIds.split("#");
						boolQuery=boolQuery.must(QueryBuilders.termsQuery("n_city_id", provinceIdsArr));
					}	
					sortBuilder = SortBuilders.fieldSort("n_inner_score").order(SortOrder.DESC);
					qb = QueryBuilders.filteredQuery(kerwordBuilder,boolQuery);	
					log.debug(qb);	
					requestBuilder = client.prepareSearch("project").setTypes("owl_project")
			    	        .setSearchType(SearchType.DFS_QUERY_THEN_FETCH).setFrom(from).setSize(pageSize);
					requestBuilder.setQuery(qb);
					requestBuilder.addSort(sortBuilder);
				}
		
			}	

						
			SearchResponse actionGet = requestBuilder.setExplain(true).execute().actionGet();	
			SearchHits searchHits =  actionGet.getHits() ;				
			
			log.debug("命中率："+searchHits.getTotalHits());
			
			for (SearchHit hit : searchHits) {
				ids.add(Long.parseLong(hit.getId()));		
				log.debug( hit.getId()+":公司名=="+hit.getSource().get("c_company_name")+":项目名=="+hit.getSource().get("c_project_name") +":内部得分=="+hit.getSource().get("n_inner_score")+":行业=="+hit.getSource().get("n_industry_id")+":城市ID=="+hit.getSource().get("n_city_id")+":融资阶段ID=="+hit.getSource().get("n_stage_id")+":团队成员=="+hit.getSource().get("c_team_name")+ ",t_last_finance_time:"+hit.getSource().get("t_last_finance_time"));
			}				
			
			long totalHits = searchHits.getTotalHits();
			Long pageCount=1l;
			long size=Long.parseLong(""+pageSize);
			if(totalHits%size==0){
				pageCount=totalHits/size;
			}else{
				pageCount=(totalHits/size)+1;
			}	
			
			newsSearchModel.setIds(ids);
			newsSearchModel.setPageCount(pageCount);
	
		log.debug(ids);
		return newsSearchModel;
	}
	
	
	/**
	 * 项目名称、标签、行业名称查找类似项目
	 * @param keywords
	 * @return
	 */
	public static SearchModel queryProjectIdsByName(int pageNow, int pageSize,String projectName){
		List<Long> ids = new ArrayList<Long>();
		SearchModel newsSearchModel = new SearchModel();
	
			QueryBuilder qb = null;
			if(Help.isNotNull(projectName)){
				qb = QueryBuilders.boolQuery().must(QueryBuilders.multiMatchQuery(projectName, "c_project_name","c_industry_name","c_tag_name"));
			}else{										
				QueryBuilder qb1 = QueryBuilders.matchAllQuery();		
//				QueryBuilder qb2 = QueryBuilders.existsQuery("c_team_scale");
				QueryBuilder qb3 = QueryBuilders.existsQuery("t_last_finance_time");		
				qb = QueryBuilders.boolQuery().should(qb1).must(qb3);				
			}
						
			Long start = System.currentTimeMillis();									
			FieldSortBuilder sortBuilder = SortBuilders.fieldSort("n_inner_score");				
			int from=(pageNow-1)*pageSize;
//			SearchHits searchHits = query("project","owl_project", from , pageSize , sortBuilder , new QueryBuilder[]{
//					qb	
//			}) ;
			
			
			log.debug(qb);
			
			SearchRequestBuilder requestBuilder = client.prepareSearch("project").setTypes("owl_project")
	    	        .setSearchType(SearchType.DFS_QUERY_THEN_FETCH).setFrom(from).setSize(pageSize);
				
			for(QueryBuilder builder:new QueryBuilder[]{
					qb	
			}){
				requestBuilder.setQuery(builder);
			}
					
			FieldSortBuilder sortBuilder1 = SortBuilders.fieldSort("n_hot_level").order(SortOrder.DESC);	
			requestBuilder.addSort( sortBuilder1 );	
//			requestBuilder.addSort( sortBuilder );	
		
			SearchResponse actionGet = requestBuilder.setExplain(true).execute().actionGet();	
			SearchHits searchHits =  actionGet.getHits() ;		
		
			Long end = System.currentTimeMillis();			
			log.debug("时间差："+(end - start));						
			for (SearchHit hit : searchHits) {
				ids.add(Long.parseLong(hit.getId()));		
				 log.debug( hit.getId()+"公司名:"+hit.getSource().get("c_company_name") + ",项目名:"+hit.getSource().get("c_project_name") + ",热度:"+hit.getSource().get("n_hot_level")+ ",团队成员:"+hit.getSource().get("c_team_name"));
			}						
			
			long totalHits = searchHits.getTotalHits();
			Long pageCount=1l;
			long size=Long.parseLong(""+pageSize);
			if(totalHits%size==0){
				pageCount=totalHits/size;
			}else{
				pageCount=(totalHits/size)+1;
			}	
			
			newsSearchModel.setIds(ids);
			newsSearchModel.setPageCount(pageCount);
			
			
		
		 log.debug(ids);
		return newsSearchModel;
	}
	
	
	
	/**
	 * 运营后台的项目列表
	 * @param keywords
	 * @return
	 */
	public static SearchModel queryProjectList(int pageNow, int pageSize,String keywords,String industryIds,String provinceIds,String cityIds,String stageIds,String financeMinTime,String financeMaxTime){
		List<Long> ids = new ArrayList<Long>();
		SearchModel newsSearchModel = new SearchModel();
	
		Long start = System.currentTimeMillis();			
		//根据in关键字搜索
		int from=(pageNow-1)*pageSize;		
					
		QueryBuilder kerwordBuilder = null;					
		QueryBuilder qb = null;
//			FieldSortBuilder sortBuilder = SortBuilders.fieldSort("c_project_name").order(SortOrder.DESC);	
		
		SearchRequestBuilder requestBuilder = null;
		
		BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
		if(Help.isNull(keywords)&&Help.isNull(industryIds)&&Help.isNull(stageIds)&&Help.isNull(provinceIds)&&Help.isNull(cityIds)&&Help.isNull(financeMinTime)&&Help.isNull(financeMaxTime)){
			
			FieldSortBuilder sortBuilder = SortBuilders.fieldSort("t_create_time").order(SortOrder.DESC);
			kerwordBuilder = QueryBuilders.boolQuery().must(QueryBuilders.matchAllQuery());
			
			qb = QueryBuilders.filteredQuery(kerwordBuilder,boolQuery);	
			log.debug(qb);			
			requestBuilder = client.prepareSearch("project").setTypes("owl_project")
	    	        .setSearchType(SearchType.DFS_QUERY_THEN_FETCH).setFrom(from).setSize(pageSize);		
			requestBuilder.setQuery(qb);
			requestBuilder.addSort(sortBuilder);
			
		}else{

			FieldSortBuilder sortBuilder = null;
		
			if(Help.isNotNull(keywords)){
				kerwordBuilder = QueryBuilders.boolQuery().must(QueryBuilders.multiMatchQuery(keywords, "c_project_name","c_company_name","c_team_name"));
			}
			if(Help.isNotNull(industryIds)){
				String[] idArr = industryIds.split("#");
				boolQuery=boolQuery.must(QueryBuilders.termsQuery("n_industry_id", idArr));
				QueryBuilders.termsQuery("n_industry_id", idArr);
			}
			
			if(Help.isNotNull(stageIds)){		
				String[] idArr = stageIds.split("#");
				boolQuery=boolQuery.must(QueryBuilders.termsQuery("n_stage_id", idArr));
			}
			
			if(Help.isNotNull(provinceIds)){		
				String[] provinceIdsArr = provinceIds.split("#");
				boolQuery=boolQuery.must(QueryBuilders.termsQuery("n_province_id", provinceIdsArr));
			}
			
			if(Help.isNotNull(cityIds)){		
				String[] provinceIdsArr = cityIds.split("#");
				boolQuery=boolQuery.must(QueryBuilders.termsQuery("n_city_id", provinceIdsArr));
			}	
			
			if(Help.isNotNull(financeMinTime) && Help.isNotNull(financeMaxTime)){								
				boolQuery=boolQuery.must(QueryBuilders.rangeQuery("t_last_finance_time").gte(financeMinTime).lt(financeMaxTime));
			}else{
				if(Help.isNotNull(financeMinTime)){
					boolQuery=boolQuery.must(QueryBuilders.rangeQuery("t_last_finance_time").gte(financeMinTime));
				}
				if(Help.isNotNull(financeMaxTime)){
					boolQuery=boolQuery.must(QueryBuilders.rangeQuery("t_last_finance_time").lt(financeMaxTime));
				}						
			}	
		
//				sortBuilder = SortBuilders.fieldSort("t_create_time").order(SortOrder.DESC);
			qb = QueryBuilders.filteredQuery(kerwordBuilder,boolQuery);	
			log.debug(qb);	
			requestBuilder = client.prepareSearch("project").setTypes("owl_project")
	    	        .setSearchType(SearchType.DFS_QUERY_THEN_FETCH).setFrom(from).setSize(pageSize);
			requestBuilder.setQuery(qb);
//				requestBuilder.addSort(sortBuilder);
			
	
		}	

					
		SearchResponse actionGet = requestBuilder.setExplain(true).execute().actionGet();	
		SearchHits searchHits =  actionGet.getHits() ;				
		
		log.debug("命中率："+searchHits.getTotalHits());
		
		for (SearchHit hit : searchHits) {
			ids.add(Long.parseLong(hit.getId()));		
			log.debug( hit.getId()+":公司名=="+hit.getSource().get("c_company_name")+":项目名=="+hit.getSource().get("c_project_name") +":内部得分=="+hit.getSource().get("n_inner_score")+":行业=="+hit.getSource().get("n_industry_id")+":城市ID=="+hit.getSource().get("n_city_id")+":融资阶段ID=="+hit.getSource().get("n_stage_id")+":团队成员=="+hit.getSource().get("c_team_name")+ ",t_last_finance_time:"+hit.getSource().get("t_last_finance_time"));
		}				
		
		long totalHits = searchHits.getTotalHits();
		Long pageCount=1l;
		long size=Long.parseLong(""+pageSize);
		if(totalHits%size==0){
			pageCount=totalHits/size;
		}else{
			pageCount=(totalHits/size)+1;
		}	
		
		newsSearchModel.setIds(ids);
		newsSearchModel.setPageCount(pageCount);
		newsSearchModel.setTotalHit(totalHits);
		
		log.debug(ids);
		return newsSearchModel;
	}
	
	/**
	 * 运营后台根据项目名称、投资事件内容查询
	 * @param keywords
	 * @return
	 */
	public static SearchModel queryFinanceIds(int pageNow, int pageSize,String keywords,String financeMinTime,String financeMaxTime){
		SearchModel newsSearchModel = new SearchModel();
		List<Long> ids = new ArrayList<Long>();
	
			Long start = System.currentTimeMillis();			
			//根据in关键字搜索
			int from=(pageNow-1)*pageSize;		
						
			QueryBuilder kerwordBuilder = null;					
			QueryBuilder qb = null;
//			FieldSortBuilder sortBuilder = SortBuilders.fieldSort("c_project_name").order(SortOrder.DESC);	
			
			SearchRequestBuilder requestBuilder = null;
			
			BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
			if(Help.isNull(keywords)&&Help.isNull(financeMinTime)&&Help.isNull(financeMaxTime)){				
				FieldSortBuilder sortBuilder = SortBuilders.fieldSort("t_finance_time").order(SortOrder.DESC);
				kerwordBuilder = QueryBuilders.boolQuery().must(QueryBuilders.matchAllQuery());
				
				qb = QueryBuilders.filteredQuery(kerwordBuilder,boolQuery);	
				log.debug(qb);			
				requestBuilder = client.prepareSearch("finance").setTypes("owl_finance")
		    	        .setSearchType(SearchType.DFS_QUERY_THEN_FETCH).setFrom(from).setSize(pageSize);		
				requestBuilder.setQuery(qb);
				requestBuilder.addSort(sortBuilder);
				
			}else{

				FieldSortBuilder sortBuilder = null;
			
				if(Help.isNotNull(keywords)){
					kerwordBuilder = QueryBuilders.boolQuery().must(QueryBuilders.multiMatchQuery(keywords, "c_project_name","c_finance_intro"));
				}
		
				
				if(Help.isNotNull(financeMinTime) && Help.isNotNull(financeMaxTime)){								
					boolQuery=boolQuery.must(QueryBuilders.rangeQuery("t_finance_time").gte(financeMinTime).lt(financeMaxTime));
				}else{
					if(Help.isNotNull(financeMinTime)){
						boolQuery=boolQuery.must(QueryBuilders.rangeQuery("t_finance_time").gte(financeMinTime));
					}
					if(Help.isNotNull(financeMaxTime)){
						boolQuery=boolQuery.must(QueryBuilders.rangeQuery("t_finance_time").lt(financeMaxTime));
					}						
				}	
			
//				sortBuilder = SortBuilders.fieldSort("t_create_time").order(SortOrder.DESC);
				qb = QueryBuilders.filteredQuery(kerwordBuilder,boolQuery);	
				log.debug(qb);	
				requestBuilder = client.prepareSearch("finance").setTypes("owl_finance")
		    	        .setSearchType(SearchType.DFS_QUERY_THEN_FETCH).setFrom(from).setSize(pageSize);
				requestBuilder.setQuery(qb);
//				requestBuilder.addSort(sortBuilder);
				
		
			}	

						
			SearchResponse actionGet = requestBuilder.setExplain(true).execute().actionGet();	
			SearchHits searchHits =  actionGet.getHits() ;				
			
			log.debug("命中率："+searchHits.getTotalHits());
			
			for (SearchHit hit : searchHits) {
				ids.add(Long.parseLong(hit.getId()));		
				log.debug( hit.getId()+"项目==="+hit.getSource().get("c_project_name")+"融资阶段==="+hit.getSource().get("c_stage_name")+"###:融资金额==="+hit.getSource().get("f_finance_money")+"###:融资事件介绍==="+hit.getSource().get("c_finance_intro"));
			}				
			
			long totalHits = searchHits.getTotalHits();
			Long pageCount=1l;
			long size=Long.parseLong(""+pageSize);
			if(totalHits%size==0){
				pageCount=totalHits/size;
			}else{
				pageCount=(totalHits/size)+1;
			}	
			
			newsSearchModel.setIds(ids);
			newsSearchModel.setPageCount(pageCount);
			newsSearchModel.setTotalHit(totalHits);
		
		 log.debug(ids);
		 return newsSearchModel;
	}
	
	/**
	 * 分组查询
	 * @param builders 搜索条件
	 * @param groupKey 分组key
	 * */
	public  static void getGroup(){
		
		SearchResponse sr = client.prepareSearch("project_industry").setTypes("owl_project_industry")
			    .addAggregation(
			        AggregationBuilders
			            .terms("projectId")
			            .field("n_industry_id")
			    )
			    .execute()
			    .actionGet();

			Terms terms = sr.getAggregations().get("projectId");
			for (Bucket gradeBucket  : terms.getBuckets()) {
			     log.debug("类型ID:"+gradeBucket.getKey() + ",对应类型的项目总数:"+ gradeBucket.getDocCount());	
			     
			     StringTerms classTerms = (StringTerms) gradeBucket.getAggregations().asMap().get("classAgg");
				 Iterator<Bucket> classBucketIt = classTerms.getBuckets().iterator();			
				 while(classBucketIt.hasNext())
				 {
					Bucket classBucket = classBucketIt.next();
					log.debug(gradeBucket.getKey() + "年级" +classBucket.getKey() + "班有" + classBucket.getDocCount() +"个学生。");
				 }			 
			}
	
			
	}
	
	
		
	public static void main(String[] args) {
		// TODO Auto-generated method stub	
		
		initEsSearch("elasticsearch","112.74.67.239",9300);
		
		String keywords = "北京易动纷享科技";		
//		int[] industryIds = new int[]{2,3,4,8};
		String industryIds = "244";
		String stageIds = "";
		String provinceIds = "";
		
		String cityIds = "";
//		log.debug(totalHits%pageSize);
//		log.debug(pageCount);
		log.debug(queryProjectIds(1,20,keywords,industryIds,stageIds,provinceIds,cityIds));
//		log.debug(queryFinanceProjectIdsInWeb(1,20,keywords,industryIds));
		
	
//		getGroup();			
	
		
		
//		log.debug(queryProjectIds(1,20,keywords,industryIds,stageIds));
		
//		log.debug(queryNewsIds(1,20,""));
	}

}
