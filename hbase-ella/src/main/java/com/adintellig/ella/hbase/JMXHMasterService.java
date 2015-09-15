package com.adintellig.ella.hbase;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.RegionLoad;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.adintellig.ella.dao.RegionDaoImpl;
import com.adintellig.ella.dao.RequestCountDaoImpl;
import com.adintellig.ella.dao.ServerDaoImpl;
import com.adintellig.ella.dao.TableDaoImpl;
import com.adintellig.ella.hbase.beans.request.MasterServiceBean;
import com.adintellig.ella.hbase.beans.request.MasterServiceBeans;
import com.adintellig.ella.hbase.beans.request.RegionServer;
import com.adintellig.ella.hbase.beans.request.RegionServerValue;
import com.adintellig.ella.hbase.beans.request.RegionsLoad;
import com.adintellig.ella.hbase.beans.request.RegionsLoadValue;
import com.adintellig.ella.model.Region;
import com.adintellig.ella.model.RequestCount;
import com.adintellig.ella.model.Server;
import com.adintellig.ella.model.Table;
import com.adintellig.ella.util.ConfigFactory;
import com.adintellig.ella.util.ConfigProperties;
import com.adintellig.ella.util.HBaseUtil;
import com.alibaba.fastjson.JSON;

public class JMXHMasterService extends Thread {
	private static Logger logger = LoggerFactory
			.getLogger(JMXHMasterService.class);

	static ConfigProperties config = ConfigFactory.getInstance()
			.getConfigProperties(ConfigFactory.ELLA_CONFIG_PATH);

	public static String url;

	private RequestCountDaoImpl reqDao = null;
	private TableDaoImpl tblDao = null;
	private RegionDaoImpl regDao = null;
	private ServerDaoImpl serDao = null;

	private static JMXHMasterService service;

	private JMXHMasterService() {
		this.reqDao = new RequestCountDaoImpl();
		this.tblDao = new TableDaoImpl();
		this.regDao = new RegionDaoImpl();
		this.serDao = new ServerDaoImpl();
	}

	public static synchronized JMXHMasterService getInstance() {
		if (service == null)
			service = new JMXHMasterService();
		return service;
	}

	public String request(String urlString) {
		URL url = null;
		BufferedReader in = null;
		StringBuffer sb = new StringBuffer();
		try {
			url = new URL(urlString);
			in = new BufferedReader(new InputStreamReader(url.openStream(),
					"UTF-8"));
			String str = null;
			while ((str = in.readLine()) != null) {
				sb.append(str);
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			try {
				if (in != null) {
					in.close();
				}
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}
		return sb.toString();
	}

	public MasterServiceBeans parseBean(String jsonString) {
		MasterServiceBeans bean = null;
		if (null != jsonString && jsonString.trim().length() > 0)
			bean = JSON.parseObject(jsonString, MasterServiceBeans.class);
		return bean;
	}

	@Override
	public void run() {
		
//		String result = request(url);
//		logger.info("Request URL: " + url);
		
		try {
			ClusterStatus status = HBaseUtil.getHbaseAdmin().getClusterStatus();
			MasterServiceBean bean = new MasterServiceBean();
			bean.setName(status.getMaster().getServerName());
			bean.setServerName(status.getMaster().getServerName());
			bean.setAverageLoad(status.getAverageLoad());
			List<RegionServer> servers = new ArrayList<RegionServer>();
			for (ServerName server : status.getServers()) {
				RegionServer s = new RegionServer();
				s.setKey(server.getServerName());
				RegionServerValue value = new RegionServerValue();
				value.setLoad(status.getLoad(server).getLoad());
				List<RegionsLoad> regionsLoads = new ArrayList<RegionsLoad>();
				for (Map.Entry<byte[], RegionLoad> entry : status.getLoad(server).getRegionsLoad().entrySet()) {
					RegionsLoad regionsLoad = new RegionsLoad();
					RegionsLoadValue regionsLoadValue = new RegionsLoadValue();
					regionsLoadValue.setNameAsString(Bytes.toStringBinary(entry.getKey()));
					regionsLoadValue.setReadRequestsCount(entry.getValue().getReadRequestsCount());
					regionsLoadValue.setRequestsCount(entry.getValue().getRequestsCount());
					regionsLoadValue.setWriteRequestsCount(entry.getValue().getWriteRequestsCount());
					regionsLoad.setValue(regionsLoadValue);
					regionsLoads.add(regionsLoad);
				}
				value.setRegionsLoad(regionsLoads);
				s.setValue(value);
				servers.add(s);
			}
			bean.setRegionServers(servers);
			MasterServiceBean[] masterServiceBeans = new MasterServiceBean[] {bean};
			MasterServiceBeans beans = new MasterServiceBeans();
			beans.setBeans(masterServiceBeans);
			// region count
			List<RequestCount> list = RequestPopulator
					.populateRegionRequestCount(beans);
			reqDao.batchAdd(list);
			logger.info("[INSERT] Load Region Count info into 'region_requests'. Size="
					+ list.size());

			// region check
			List<Region> regions = RequestPopulator.populateRegions(list);
			if (regDao.needUpdate(regions)) {
				regDao.truncate();
				regDao.batchUpdate(regions);
			}

			// server count
			list = RequestPopulator.populateRegionServerRequestCount(beans);
			reqDao.batchAdd(list);
			logger.info("[INSERT] Load Server Count info into 'server_requests'. Size="
					+ list.size());

			// server check
			List<Server> servers_ = RequestPopulator.populateServers(list);
			if (serDao.needUpdate(servers_)) {
				serDao.truncate();
				serDao.batchUpdate(servers_);
			}

			// table count
			list = RequestPopulator.populateTableRequestCount(beans);
			reqDao.batchAdd(list);
			logger.info("[INSERT] Load Table Count info into 'table_requests'. Size="
					+ list.size());

			// table check
			List<Table> tables = RequestPopulator.populateTables(list);
			if (tblDao.needUpdate(tables)) {
				tblDao.truncate();
				tblDao.batchUpdate(tables);
			}

		} catch (SQLException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public static void main(String[] args) throws JsonParseException,
			JsonMappingException, IOException, SQLException {
		JMXHMasterService service = JMXHMasterService.getInstance();
		Thread t = new Thread(service);
		t.start();
	}
}
