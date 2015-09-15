package com.adintellig.ella.hbase.beans.request;

import java.util.List;

public class RegionServerValue {
	
	private Integer load;
	private List<RegionsLoad> regionsLoad;

	public Integer getLoad() {
		return load;
	}

	public void setLoad(Integer load) {
		this.load = load;
	}

	public List<RegionsLoad> getRegionsLoad() {
		return regionsLoad;
	}

	public void setRegionsLoad(List<RegionsLoad> regionsLoad) {
		this.regionsLoad = regionsLoad;
	}

}
