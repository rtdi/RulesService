package io.rtdi.appcontainer.utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import io.rtdi.bigdata.rulesservice.RulesService;

public class UsageStatistics {

	private List<Connection> connections;
	private long starttime;
	private long endtime;

	public UsageStatistics(RulesService serviceListener) {
		connections = Collections.singletonList(new Connection(serviceListener));
		starttime = System.currentTimeMillis();
	}

	public String getConnectorname() {
		return "RulesService";
	}

	public List<Connection> getConnections() {
		return connections;
	}

	public long getStarttime() {
		return starttime;
	}

	public long getEndtime() {
		return endtime;
	}

	public void update() {
		connections.get(0).update();
		starttime = endtime;
		endtime = System.currentTimeMillis();
	}

	public static class Connection {
		private String connectionname;
		private List<Producer> producers;

		public Connection(RulesService serviceListener) {
			this.connectionname = serviceListener.getBootstrapServers();
			producers = new ArrayList<>();
			producers.add(new Producer("Rules", serviceListener));
			update();
		}
		
		public String getConnectionname() {
			return connectionname;
		}

		public List<Producer> getProducers() {
			return producers;
		}

		public void update() {
			producers.get(0).update();
		}

	}
	
	public static class Producer {

		private String producername;
		private List<ProducerInstance> instances;

		public Producer(String name, RulesService serviceListener) {
			this.producername = name;
			instances = Collections.singletonList(new ProducerInstance(serviceListener));
		}

		public String getProducername() {
			return producername;
		}

		public List<ProducerInstance> getInstances() {
			return instances;
		}

		public int getInstancecount() {
			return 1;
		}
		
		public void update() {
			instances.get(0).update();
		}

	}
	
	public static class ProducerInstance {

		private Long lastdatatimestamp;
		private String state;
		private long rowsproduced;
		private RulesService service;

		public ProducerInstance(RulesService serviceListener) {
			this.service = serviceListener;
		}

		public void update() {
			rowsproduced = service.getRowsProduced();
			state = service.getState();
			lastdatatimestamp = service.getLastDataTimestamp();
		}

		public Long getLastdatatimestamp() {
			return lastdatatimestamp;
		}

		public String getState() {
			return state;
		}

		public long getRowsproduced() {
			return rowsproduced;
		}

	}
	
}
