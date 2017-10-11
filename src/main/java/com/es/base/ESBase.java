package com.es.base;

import java.net.InetAddress;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

public class ESBase {
	
	protected static Client client;
	
	//项目index及其type
	protected static String indexPro = "project";
	protected static String typePro = "owl_project";
	protected static String typeProIndustry = "owl_project_industry";
	protected static String typeProFinance = "owl_project_finance";
	protected static String typeProFav = "owl_project_fav";
	protected static String typeProLicense = "owl_project_license";
	protected static String typeIndustry = "owl_industry";
	
	//新闻index及其type
	protected static String indexNews = "news";
	protected static String typeNews = "owl_news";
	
	//团队index及其type
	protected static String indexTeam = "team";
	protected static String typeTeam = "owl_team";
	
	
	/**
	 * 创建客户端
	 * @param clusterName
	 * @param host
	 * @param port
	 * @return
	 * @throws Exception
	 */
	public static void getClient(String clusterName,String host,Integer port) throws  Exception{
		Settings settings = Settings.settingsBuilder()
		        .put("cluster.name", clusterName).put("client.transport.sniff", true).build();
		client = TransportClient.builder().settings(settings).build()
		        .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), port));	
	}
	



}
