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

import java.io.File;
import java.io.FileInputStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.zookeeper.server.util.VerifyingFileFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZNSysTestConfig {
    private static final Logger LOG = LoggerFactory.getLogger(ZNSysTestConfig.class);
    private static final DateFormat dateFormat = new SimpleDateFormat("MM.dd.HH:mm:ss");

    public String generateTestName() {
    	return dateFormat.format(new Date())+".log";
    }
    
    public static class ServerConnectionInfo {
    	String zkid;
    	String containerName[];
    	String serverIdInContainer[];
    	
    	public ServerConnectionInfo(String infostr){
    		zkid=infostr.split("\\-")[0];
    		String[] containerNameAndServerIDpairs = infostr.split("\\-")[1].split(",");
    		containerName = new String[containerNameAndServerIDpairs.length];
    		serverIdInContainer= new String[containerNameAndServerIDpairs.length];
    		for (int i=0; i<containerNameAndServerIDpairs.length; i++){
    			containerName[i] = containerNameAndServerIDpairs[i].split("\\:")[0];
    			serverIdInContainer[i] = containerNameAndServerIDpairs[i].split("\\:")[1];
    		}
    	}
    	
    	public String toString(){
    		StringBuilder sb = new StringBuilder();
    		sb.append("Name:"+zkid + " Containers:");
    		for (int i=0; i< containerName.length; i++){
                sb.append( containerName[i]+ ":"+serverIdInContainer[i] + " " );
            }
    		return sb.toString();
    	}
    }
    
    public static class AppInfo implements Comparable<AppInfo>{
        public AppInfo(String name, 
                int readPercent, 
                int localPercent, 
                int bytesSize,
                int clients,
                String localServerInfo,
                String[] containerNames,
                String[] remoteServersInfos){
            this.appName = name;
            this.readPercent = readPercent;
            this.localPercent = localPercent;
            this.bytesSize = bytesSize;
            this.clients = clients;
            this.localServerInfo = new ServerConnectionInfo(localServerInfo);
            if (remoteServersInfos != null){
            	this.remoteServersInfos = new ServerConnectionInfo[remoteServersInfos.length];
	            for (int i=0; i<remoteServersInfos.length; i++){
	            	this.remoteServersInfos[i] = new ServerConnectionInfo(remoteServersInfos[i]);
	            }
            } else {
            	this.remoteServersInfos = new ServerConnectionInfo[0];
            }
            this.containerNames = containerNames;
        }
        
        public String appName;
        
        public int readPercent;
        public int localPercent;
        public int bytesSize;
        
        // This amount of clients that act according to the percentages will in the stated ZK.
        public int clients;  
        
        public String partitions;
        public ServerConnectionInfo localServerInfo; // To which the clients connect
        public String[] containerNames; // From which we ran the clients
        public ServerConnectionInfo[] remoteServersInfos; // To which the clients connect
        private int nextContainerIndex = 0;
        
        public String getNextContainer() {
            return containerNames[nextContainerIndex++ % containerNames.length];
        }

        @Override
        public int compareTo(AppInfo o) {
            return appName.compareTo(o.appName);
        }
    }
    
    public static class ZKInfo implements Comparable<ZKInfo>{
    	public String zkName;
        // Each element is "containerName:observer/participant"
        public String[] serverInfoAndType;
        public String quorumConnectionString;
        // Keys are container names, values are comma separated strings that contain all the servers belong to ZK that run by that container.
        public HashMap<String, String> containerAndServerIdToConnectionString;
        public String serverInfoAndTypeString() {
            StringBuilder s = new StringBuilder();
            for (int i=0; i< serverInfoAndType.length; ++i){
                s.append(serverInfoAndType[i].split("\\:")[2].charAt(0));
            }
            return s.toString();
        }
		@Override
		public int compareTo(ZKInfo o) {
			return this.zkName.compareTo(o.zkName);
		}
    }   

    public SortedSet<AppInfo> apps;  
    public SortedSet<ZKInfo> zks;
    public HashMap<String,ZKInfo> zkidToZKInfo;
    public String mdzk;
    public String perfix;
    public int secToLeaderElection = 10;
    public boolean printForGraph = false;
    public int burstSize = 10;
    public int outstandingLimit = 100;
    public int binsNumber = 100;
    public boolean leaderServes = false;
    public String testIP;
    
    public ZNSysTestConfig(String path) {
        try{    
            File configFile = (new VerifyingFileFactory.Builder(LOG)
                .warnForRelativePath()
                .failForNonExistingPath()
                .build()).create(path);
                
            Properties cfg = new Properties();
            FileInputStream in = new FileInputStream(configFile);
            try {
                cfg.load(in);
            } finally {
                in.close();
            }
            System.out.println("Config -"+cfg.toString());
            parseProperties(cfg);
        }
        catch(Exception e){
            System.err.println(e.toString());
        }
    }
    
    public String fullConfigString(){
        StringBuilder cfg = new StringBuilder();
        cfg.append("mdzk address "+mdzk + "\n");
        cfg.append("perfix "+perfix + "\n");
        cfg.append("leaderServes "+leaderServes + "\n");
        cfg.append("burst size "+burstSize+ "\n");
        cfg.append("outstanding limit"+outstandingLimit+ "\n");
        cfg.append("latency bins number "+binsNumber+ "\n");
        
        for (AppInfo ai : apps){
            cfg.append(
                    ai.appName + ":" + 
                    ai.readPercent + ":" +
                    ai.localPercent + ":" +
                    ai.bytesSize + ":" +
                    ai.clients +"\n");
            cfg .append( ai.appName + ":" +"Using containers:");
            cfg .append( ai.localServerInfo.toString() + " ");
            cfg .append("\n");
        }
        for (ZKInfo zk : zks){
            cfg.append(zk.zkName + "_" +
        			zk.serverInfoAndType.length+"\n");
        }
        return cfg.toString();
    }
    
    private void parseProperties(Properties cfg) {
        apps = new TreeSet<AppInfo>();
        zks = new TreeSet<ZKInfo>();
        zkidToZKInfo = new HashMap<String, ZKInfo>();
        for (Entry<Object, Object> entry : cfg.entrySet()) {
            String key = entry.getKey().toString().trim();
            String value = entry.getValue().toString().trim();
            if (key.startsWith("AppInfo")){
                addNewApp(value);
            } else if (key.startsWith("ZKInfo_Servers")){
                addZK(value);
            } else if (key.startsWith("metaDataZK")){
                this.mdzk = value;
            } else if (key.startsWith("testPrefix")){
                this.perfix = value;
            } else if (key.startsWith("secToLeaderElection")){
                this.secToLeaderElection = Integer.parseInt(value);
            } else if (key.startsWith("printOnlyForGraph")){
            	this.printForGraph = Boolean.parseBoolean(value);
            } else if (key.startsWith("burstSize")){
            	this.burstSize = Integer.parseInt(value);
            } else if (key.startsWith("outstandingLimit")){
            	this.outstandingLimit = Integer.parseInt(value);
            } else if (key.startsWith("binsNumber")){
            	this.binsNumber= Integer.parseInt(value);
            } else if (key.startsWith("leaderServes")){
            	this.leaderServes= Boolean.parseBoolean(value);
            } else if (key.startsWith("testIP")){
            	this.testIP= value;
            }
        }
    }

//  appStr=
//    <app name (for debug)>|
//	  <read percentage>-<local percentage>|
//	  <bytes size>|
//	  <#appClients>|
//	  <Local ZK name>-[containers:idNumberOfServer list]| -- for the local ZK
//	  [container names]| -- from which the clients run
//	  <Remote ZK name>-[containers:idNumberOfServer list] list -- for each remote ZK, list of containers, lists of different ZKs separated by ";"
    private void addNewApp(String appStr) {
        String[] appInfo = appStr.split("\\|");
        String[] parameters = appInfo[1].split("\\-");
        AppInfo ai = new AppInfo(
                appInfo[0], 
                Integer.parseInt(parameters[0]),
                Integer.parseInt(parameters[1]),
                Integer.parseInt(appInfo[2]),
                Integer.parseInt(appInfo[3]),
                appInfo[4],
                appInfo[5].split("\\,"),
                appInfo.length == 7 ? appInfo[6].split("\\;") : null);
        this.apps.add(ai);
    }  
    
    // ZKInfo_Servers=<zk name>|list of <[instance container name]>:idNumberOfServer:<"observer"/"participant">;...
    private void addZK(String zkServers) {
        String[] zks = zkServers.split("\\|");
        ZKInfo zk = new ZKInfo();
        zk.zkName = zks[0];
        zk.serverInfoAndType = zks[1].split("\\;");
        System.out.println("Adding ZK:"+ zk.zkName+" value-"+zk);
        zkidToZKInfo.put(zk.zkName, zk);
        this.zks.add(zk);
    }
}
