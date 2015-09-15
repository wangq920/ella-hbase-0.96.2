package com.adintellig.ella.hbase.beans.request;

import java.util.List;

public class MasterServiceBean {
	private String name;
	private String ServerName;
	private double AverageLoad;
	private List<RegionServer> RegionServers;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getServerName() {
		return ServerName;
	}

	public void setServerName(String serverName) {
		ServerName = serverName;
	}

	public double getAverageLoad() {
		return AverageLoad;
	}

	public void setAverageLoad(double averageLoad) {
		AverageLoad = averageLoad;
	}

	public List<RegionServer> getRegionServers() {
		return RegionServers;
	}

	public void setRegionServers(List<RegionServer> regionServers) {
		RegionServers = regionServers;
	}

}
