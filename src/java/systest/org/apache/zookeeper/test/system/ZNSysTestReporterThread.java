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

import java.util.Calendar;
import java.util.HashMap;

import org.apache.zookeeper.test.system.ZNSysTestConfig.AppInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ZNSysTestReporterThread extends Thread {
	protected static final Logger LOG = LoggerFactory.getLogger(ZNSysTestReporterThread.class);

	ZNSysTestConfig config;

	public ZNSysTestReporterThread(ZNSysTestConfig config) {
		this.config = config;
//		setDaemon(true);
		start();
	}

	public void run() {
		try {

			HashMap<String, Long> min = new HashMap<String, Long>();
			HashMap<String, Long> max = new HashMap<String, Long>();
			HashMap<String, Long> totalRequests = new HashMap<String, Long>();
			HashMap<String, Long> totalLocalReads = new HashMap<String, Long>();
			HashMap<String, Long> totalLocalWrites = new HashMap<String, Long>();
			HashMap<String, Long> totalRemoteReads = new HashMap<String, Long>();
			HashMap<String, Long> totalRemoteWrites = new HashMap<String, Long>();
			HashMap<String, Long> intervals = new HashMap<String, Long>();

			for (AppInfo app : config.apps){
				ZNSysTest.currentIntervalByAppName.put(app.appName, System.currentTimeMillis() / ZNSysTestInterval.INTERVAL);
				min.put(app.appName, (long) 999999999);
				max.put(app.appName, (long) 0);
				totalRequests.put(app.appName, (long) 0);
				totalLocalReads.put(app.appName, (long) 0);
				totalLocalWrites.put(app.appName, (long) 0);
				totalRemoteReads.put(app.appName, (long) 0);
				totalRemoteWrites.put(app.appName, (long) 0);
				intervals.put(app.appName, (long) 0);
			}

			// Give things time to report;
			Thread.sleep(ZNSysTestInterval.INTERVAL * 2);

			outloop:
				while (true) {
					Thread.sleep((long) ZNSysTestInterval.INTERVAL);
					int appCounter = 0;
					if (!config.printForGraph){
						System.err.println("-------------------------------------------------------------------");
					}
					for (AppInfo app : config.apps){
						appCounter++;
						long lastInterval = ZNSysTest.currentIntervalByAppName.get(app.appName);
						ZNSysTest.currentIntervalByAppName.put(app.appName, lastInterval + 1);

						ZNSysTestInterval interval = ZNSysTest.remove(app.appName, lastInterval);

						if (interval == null){
							if (appCounter < config.apps.size()){
								continue;
							}
							else {
								Thread.sleep((long) ZNSysTestInterval.INTERVAL);
								continue outloop;
							}
						}

						long requetsInCurrentInterval = interval.localReadRequests + interval.localWriteRequests + interval.remoteReadRequests + interval.remoteWriteRequests;
						//                System.out.println(interval.toString());
						requetsInCurrentInterval = (requetsInCurrentInterval * 1000) / ZNSysTestInterval.INTERVAL; // Multiply by 1000 to get reqs/sec
						if (requetsInCurrentInterval < min.get(app.appName)) {
							min.put(app.appName, (long) requetsInCurrentInterval);
						}
						if (requetsInCurrentInterval > max.get(app.appName)) {
							max.put(app.appName, (long) requetsInCurrentInterval);
						}
						totalRequests.put(app.appName, (long) (totalRequests.get(app.appName) + requetsInCurrentInterval));
						totalLocalReads.put(app.appName, totalLocalReads.get(app.appName) + ((interval.localReadRequests * 1000) / ZNSysTestInterval.INTERVAL));
						totalLocalWrites.put(app.appName, totalLocalWrites.get(app.appName) + ((interval.localWriteRequests* 1000) / ZNSysTestInterval.INTERVAL));
						totalRemoteReads.put(app.appName, totalRemoteReads.get(app.appName) + ((interval.remoteReadRequests * 1000) / ZNSysTestInterval.INTERVAL));
						totalRemoteWrites.put(app.appName, totalRemoteWrites.get(app.appName) + ((interval.remoteWriteRequests * 1000) / ZNSysTestInterval.INTERVAL));
						intervals.put(app.appName, intervals.get(app.appName) + 1);

						Calendar calendar = Calendar.getInstance();
						calendar.setTimeInMillis((long) (lastInterval * ZNSysTestInterval.INTERVAL));
						String report;
						if (!config.printForGraph){
							String intervalStatus = 
									"Interval: " + lastInterval + " "
											+ calendar.get(Calendar.HOUR_OF_DAY) + ":"
											+ calendar.get(Calendar.MINUTE) + ":"
											+ calendar.get(Calendar.SECOND)
											+ "\n" ;

							String appName = 
									"App name: " + app.appName + " "
											+ "App [%reads|%locals]:[" 
											+ app.readPercent + "|"
											+ app.localPercent + "]"
											+ "\n";

							String throughputStatus = 
									"Total Throughput: "
											+ requetsInCurrentInterval + " "
											+ "| " +
											+ min.get(app.appName) + " "
											+ ((double) totalRequests.get(app.appName) / (double) intervals.get(app.appName)) + " "
											+ max.get(app.appName)
											+ "\n" ;
							String localReadThroughputStatus = 
									"Local Read Throughput: "
											+ ((double)(interval.localReadRequests * 1000)) / (double) ZNSysTestInterval.INTERVAL + " "
											+ "| " +
											+ ((double) totalLocalReads.get(app.appName)/ (double) intervals.get(app.appName)) + " "
											+ "\n" ;
							String localWriteThroughputStatus = 
									"Local Write Throughput: "
											+ (double)(interval.localWriteRequests * 1000) / (double) ZNSysTestInterval.INTERVAL + " "
											+ "| " +
											+ ((double) totalLocalWrites.get(app.appName)/ (double) intervals.get(app.appName)) + " "
											+ "\n" ;
							String remoteReadThroughputStatus = 
									"Remote Read Throughput: "
											+ (double)(interval.remoteReadRequests * 1000) / (double) ZNSysTestInterval.INTERVAL + " "
											+ "| " +
											+ ((double) totalRemoteReads.get(app.appName)/ (double) intervals.get(app.appName)) + " "
											+ "\n" ;
							String remoteWriteThroughputStatus = 
									"Remote Write Throughput: "
											+ (double)(interval.remoteWriteRequests* 1000)/ (double) ZNSysTestInterval.INTERVAL + " "
											+ "| " +
											+ ((double) totalRemoteWrites.get(app.appName)/ (double) intervals.get(app.appName)) + " "
											+ "\n" ;

							report = 
									"\n" 
											+ intervalStatus 
											+ appName
											+ throughputStatus
											+ localReadThroughputStatus
											+ localWriteThroughputStatus
											+ remoteReadThroughputStatus 
											+ remoteWriteThroughputStatus
											;

						} else {
							report =  lastInterval +"\t"
									+ app.appName + "\t"
									+ interval.localReadRequests + "\t"
									+ interval.localWriteRequests + "\t"
									+ interval.remoteReadRequests + "\t"
									+ interval.remoteWriteRequests + "\t"
									+ "Meadian:\t"
									+ interval.localReadLatency.quantile(0.5)+ "\t"
									+ interval.localWriteLatency.quantile(0.5)+ "\t"
									+ interval.remoteReadLatency.quantile(0.5)+ "\t"
									+ interval.remoteWriteLatency.quantile(0.5)+ "\t"
									+ "0.95:\t"
									+ interval.localReadLatency.quantile(0.95)+ "\t"
									+ interval.localWriteLatency.quantile(0.95)+ "\t"
									+ interval.remoteReadLatency.quantile(0.95)+ "\t"
									+ interval.remoteWriteLatency.quantile(0.95)+ "\t"
									+ "0.99:\t"
									+ interval.localReadLatency.quantile(0.99)+ "\t"
									+ interval.localWriteLatency.quantile(0.99)+ "\t"
									+ interval.remoteReadLatency.quantile(0.99)+ "\t"
									+ interval.remoteWriteLatency.quantile(0.99)+ "\t"
									;
						}
						System.err.println(report);
						if (ZNSysTest.sf != null) {
							ZNSysTest.sf.println(report);
						}
					}
				}
		} catch (Exception e) {
			LOG.error("ReporterThread");
			e.printStackTrace();
		}
	}
}