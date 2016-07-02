/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zookeeper.test.system;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.test.system.ZNSysTestConfig.AppInfo;
import org.apache.zookeeper.test.system.ZNSysTestConfig.ServerConnectionInfo;
import org.apache.zookeeper.test.system.ZNSysTestConfig.ZKInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZNSysTest {

	private static final Logger LOG = LoggerFactory.getLogger(ZNSysTest.class);

	static ServerSocket ss;
	static Set<ZNSysTestSlaveThread> slaves = Collections.synchronizedSet(new HashSet<ZNSysTestSlaveThread>());
	volatile static HashMap<String, Long> currentIntervalByAppName = new HashMap<String, Long>();
	static PrintStream sf;
	static PrintStream tf;
	static {
		try {
			tf = new PrintStream(new FileOutputStream("trace"));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	static private HashMap<String, HashMap<Long, Long>> localReadByTime = new HashMap<String, HashMap<Long, Long>>();
	static private HashMap<String, HashMap<Long, Long>> localWriteByTime = new HashMap<String, HashMap<Long, Long>>();
	static private HashMap<String, HashMap<Long, Long>> remoteReadByTime = new HashMap<String, HashMap<Long, Long>>();
	static private HashMap<String, HashMap<Long, Long>> remoteWriteByTime = new HashMap<String, HashMap<Long, Long>>();
	static private HashMap<String, HashMap<Long, NumericHistogram>> localReadLatencyByTime = new HashMap<String, HashMap<Long, NumericHistogram>>();
	static private HashMap<String, HashMap<Long, NumericHistogram>> localWriteLatencyByTime = new HashMap<String, HashMap<Long, NumericHistogram>>();
	static private HashMap<String, HashMap<Long, NumericHistogram>> remoteReadLatencyByTime = new HashMap<String, HashMap<Long, NumericHistogram>>();
	static private HashMap<String, HashMap<Long, NumericHistogram>> remoteWriteLatencyByTime = new HashMap<String, HashMap<Long, NumericHistogram>>();


	synchronized static ZNSysTestInterval remove(String appName, long interval) {
		Long localReads = localReadByTime.get(appName).remove(interval);
		Long localWrites = localWriteByTime.get(appName).remove(interval);
		Long remoteReads = remoteReadByTime.get(appName).remove(interval);
		Long remoteWrites = remoteWriteByTime.get(appName).remove(interval);
		NumericHistogram localReadsLatency = localReadLatencyByTime.get(appName).remove(interval);
		NumericHistogram localWritesLatency = localWriteLatencyByTime.get(appName).remove(interval);
		NumericHistogram remoteReadsLatency = remoteReadLatencyByTime.get(appName).remove(interval);
		NumericHistogram remoteWritesLatency = remoteWriteLatencyByTime.get(appName).remove(interval);

		if (localReads == null && localWrites == null &&  remoteReads == null &&  remoteWrites == null){
			return null;
		}
		return new ZNSysTestInterval(localReads, localWrites, remoteReads, remoteWrites, localReadsLatency, localWritesLatency, remoteReadsLatency, remoteWritesLatency);
	}

	synchronized static void add(
			String appName, long time, 
			int localReads, int localWrites, int remoteReads, int remoteWrites,
			int[] localReadsLatency, int[] localWritesLatency, int[] remoteReadsLatency, int[] remoteWritesLatency,
			Socket s) {
		long interval = time / ZNSysTestInterval.INTERVAL;
		if (currentIntervalByAppName.get(appName) == null  || currentIntervalByAppName.get(appName) == 0 || currentIntervalByAppName.get(appName) > interval) {
			System.out.println(
					"Dropping " + localReads+" "+ localWrites+" "+remoteReads+" "+remoteWrites +
					" for " + new Date(time)+ " " 
					+ (currentIntervalByAppName.get(appName) == null ? "" :  + currentIntervalByAppName.get(appName)) 
					+ ">" + interval);
			return;
		}
		// We track totals by seconds
		addToThrouputMap(interval, localReads, localReadByTime.get(appName));
		addToThrouputMap(interval, localWrites, localWriteByTime.get(appName));
		addToThrouputMap(interval, remoteReads, remoteReadByTime.get(appName));
		addToThrouputMap(interval, remoteWrites, remoteWriteByTime.get(appName));
		addToLatencyMap(interval, localReadsLatency, localReadLatencyByTime.get(appName));
		addToLatencyMap(interval, localWritesLatency, localWriteLatencyByTime.get(appName));
		addToLatencyMap(interval, remoteReadsLatency, remoteReadLatencyByTime.get(appName));
		addToLatencyMap(interval, remoteWritesLatency, remoteWriteLatencyByTime.get(appName));
	}

	static private void addToThrouputMap(long interval, int count, Map<Long, Long> throuputMap){
		Long total = throuputMap.get(interval);
		if (total == null) {
			throuputMap.put(interval, (long) count);
		} else {
			throuputMap.put(interval, total.longValue() + count);
		}
	}

	static private void addToLatencyMap(long interval, int[] latency, Map<Long, NumericHistogram> latencyMap){
		NumericHistogram currLatency = latencyMap.get(interval);
		if (currLatency == null) {
			NumericHistogram nh = new NumericHistogram(latency);
			latencyMap.put(interval, nh);
		} else {
			currLatency.merge(latency);
			latencyMap.put(interval, currLatency);
		}
	}

	public static void main(String[] args) throws InterruptedException,
	KeeperException, NoAvailableContainers, DuplicateNameException,
	NoAssignmentException {

		if (args.length != 1) {
			doUsage();
			return;
		}

		ZNSysTestConfig config = new ZNSysTestConfig(args[0]);
		LOG.info(config.fullConfigString());
		try{
			ZNSysTestStatusWatcher statusWatcher = new ZNSysTestStatusWatcher();
			ZooKeeper testMetadataZK = new ZooKeeper(config.mdzk, 15000, statusWatcher);
			if (!statusWatcher.waitConnected(5000)) {
				System.err.println("Could not connect to metadata ZK:" + config.mdzk);
				return;
			}

			InstanceManager im = new InstanceManager(testMetadataZK, config.perfix);
			ss = new ServerSocket(0);
			int port = ss.getLocalPort();

			prepareAndStartZKs(config, im);

			Thread.sleep(config.secToLeaderElection * 1000);

			prepareClients(config, im, port);

			sf = new PrintStream(config.generateTestName());

			new ZNSysTestAcceptorThread();
			new ZNSysTestReporterThread(config);
		} catch(Exception e){
			LOG.error("main");
			e.printStackTrace();
		}
	}

	private static void prepareAndStartZKs(ZNSysTestConfig config, InstanceManager im) 
			throws NoAvailableContainers, DuplicateNameException, InterruptedException, KeeperException, NoAssignmentException, UnknownHostException {
		for (ZKInfo zk : config.zks){
			zk.containerAndServerIdToConnectionString = new HashMap<String, String>();
			StringBuilder quorumHostPort = new StringBuilder();

			for (int serverIndex = 0; serverIndex < zk.serverInfoAndType.length; ++serverIndex){
				String[] currServer = zk.serverInfoAndType[serverIndex].split("\\:");
				String container = currServer[0];
				String serverId = currServer[1];
				String type = currServer[2];
				String serverIP = currServer[3];
				String r[] = 
						QuorumPeerInstance.createServerAtContainer(
								im, container,
								serverId, type, config.leaderServes, serverIP);

				if (serverIndex > 0) {
					quorumHostPort.append(',');
				}
				String serverInfo = container+":"+serverId;
				String connectionOfContainer = zk.containerAndServerIdToConnectionString.get(serverInfo);
				if (connectionOfContainer == null){
					zk.containerAndServerIdToConnectionString.put(serverInfo, r[0]);
				}
				else{
					zk.containerAndServerIdToConnectionString.put(serverInfo, connectionOfContainer + ","+r[0]); 
				}

				quorumHostPort.append(r[1]);           // r[1] == "host:leaderPort:leaderElectionPort:type:serverId"
				quorumHostPort.append(";"+(r[0].split("\\:"))[1]); // Appending ";clientPort"
			}
			zk.quorumConnectionString = quorumHostPort.toString();


			System.out.println("Starting ZK at:"+zk.quorumConnectionString);
			for (int serverIndex = 0; serverIndex < zk.serverInfoAndType.length; ++serverIndex) {
				String serverId = zk.serverInfoAndType[serverIndex].split("\\:")[1];
				System.out.println("Starting server "+serverIndex+" with server id "+serverId);
				QuorumPeerInstance.startInstance(im, zk.quorumConnectionString, serverId);
				Thread.sleep(1000);
			} 
		}
	}

	private static void prepareClients(ZNSysTestConfig config, InstanceManager im, int port) throws UnknownHostException, NoAvailableContainers, DuplicateNameException, InterruptedException, KeeperException {
		for (AppInfo app : config.apps){
			localReadByTime.put(app.appName, new HashMap<Long, Long>());
			localWriteByTime.put(app.appName, new HashMap<Long, Long>());
			remoteReadByTime.put(app.appName, new HashMap<Long, Long>());
			remoteWriteByTime.put(app.appName, new HashMap<Long, Long>());
			localReadLatencyByTime.put(app.appName, new HashMap<Long, NumericHistogram>());
			localWriteLatencyByTime.put(app.appName, new HashMap<Long, NumericHistogram>());
			remoteReadLatencyByTime.put(app.appName, new HashMap<Long, NumericHistogram>());
			remoteWriteLatencyByTime.put(app.appName, new HashMap<Long, NumericHistogram>());


			// We give each client a bit different local connection array so that they'll try to connect one server
			// after another, not by the same order.
			// For the remote ZKs, we just place them one after another with "|"
			// The assumption is that the first string belong to the local ZK, and after "|" we have the remote ZKs, also separated by "|"
			for (int clientID=0; clientID < app.clients; ++clientID){
				String[] connectionArray = createLocalConnectionArray(app, config);
				String clientLocalConnectionString = createLocalConnectionString(clientID, connectionArray);
				String clientRemoteConnectionString = createRemoteConnectionString(clientID, app, config);
				StringBuilder clientParams = new StringBuilder();
				clientParams.append(
						clientLocalConnectionString + "|"+ clientRemoteConnectionString + ' ' +      // parts[0] 
						app.readPercent+"|"+app.localPercent+"|" + ' ' + // parts[1]
						app.bytesSize + ' ' +               // parts[2]
						app.appName + ' ' +                 // parts[3]
						config.testIP + ':' + port + ' ' + //parts[4]
						config.burstSize + ' ' + //parts[5]
						config.outstandingLimit+ ' ' + //parts[6]
						config.binsNumber + ' ' //parts[7]
						);

				String containerName = app.getNextContainer();
				LOG.info("Creating new client of app "+app.appName+" at container "+containerName);
				im.assignSpecificInstance(
						containerName,
						"App_"+app.appName + "_client" + clientID,  
						ZNSysTestInstance.class, 
						clientParams.toString());
			}
		}
	}

	// Different remote ZKs are concatenated with ";" 
	private static String createRemoteConnectionString(int clientID, AppInfo app, ZNSysTestConfig config){
		StringBuilder res = new StringBuilder();
		for (ServerConnectionInfo s : app.remoteServersInfos){
			for(int i=0; i<s.containerName.length; ++i){
				if (res.length() > 0){
					res.append(",");
				}
				res.append(s.zkid+"~");
				res.append(config.zkidToZKInfo.get(s.zkid)
						.containerAndServerIdToConnectionString.get(s.containerName[i]+":"+s.serverIdInContainer[i]));
			}
			res.append(";");
		}
		return res.toString();
	}

	private static String createLocalConnectionString(int clientID, String[] connectionArray) {
		int startIndex = clientID % connectionArray.length;
		StringBuilder res = new StringBuilder();
		for (int i=startIndex; i< connectionArray.length; ++i){
			if (res.length()>0){
				res.append(",");
			}
			res.append(connectionArray[i]);
		}
		for (int i=0; i< startIndex; ++i){
			if (res.length() > 0){
				res.append(",");
			}
			res.append(connectionArray[i]);
		}
		return res.toString();
	}

	private static String[] createLocalConnectionArray(AppInfo app, ZNSysTestConfig config) {
		ServerConnectionInfo local = app.localServerInfo;
		String[] res = new String[local.containerName.length];
		System.out.println("Local = "+local.toString());
		ZKInfo localZK = config.zkidToZKInfo.get(local.zkid);
		for (int i=0; i< local.containerName.length; ++i){
			res[i]= localZK.containerAndServerIdToConnectionString.get(local.containerName[i]+":"+local.serverIdInContainer[i]);
		}
		return res;
	}

	private static void doUsage() {
		System.err.println("USAGE: " + ZNSysTest.class.getName() + " <path to config file>");
		System.exit(2);
	}
}
