package com.es.model;

import java.util.List;

import com.owl.common.util.Help;

/**
 * 返回查询后的IDS值
 * @author Auser
 *
 */
public class SearchModel {
	private List<Long> ids;
	private Long pageCount=1l;
	
	private Long totalHit;

	public Long getPageCount() {
		return pageCount;
	}

	public void setPageCount(Long pageCount) {
		if(Help.isNotNull(pageCount)){
			this.pageCount = pageCount;
		}else{
			this.pageCount = 1l;
		}		
	}

	public List<Long> getIds() {
		return ids;
	}

	public void setIds(List<Long> ids) {
		this.ids = ids;
	}

	public Long getTotalHit() {
		return totalHit;
	}

	public void setTotalHit(Long totalHit) {
		this.totalHit = totalHit;
	}

	
	
}
