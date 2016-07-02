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

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketException;
import java.util.Arrays;
import java.util.Random;

import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooNet;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ZNSysTestInstance implements Instance {
	protected static final Logger LOG = LoggerFactory.getLogger(ZNSysTestInstance.class);

	byte bytes[];

	int readsPercent = -1;
	int localPercent = -1;
	String appName;
	int burstSize = 1;
	int outstandingLimit = 100;
	int binsNumber = 100;

	long lastUpdate = 0;


	final Object statSync = new Object();

	int errors;
	int localReadFinished;
	int localWriteFinished;
	int remoteReadFinished;
	int remoteWriteFinished;
	NumericHistogram localReadLatency;
	NumericHistogram localWriteLatency;
	NumericHistogram remoteReadLatency;
	NumericHistogram remoteWriteLatency;

	void resetTestCounters(){
		errors = localReadFinished = localWriteFinished = remoteReadFinished = remoteWriteFinished = 0;
		localReadLatency = new NumericHistogram(binsNumber);
		localWriteLatency = new NumericHistogram(binsNumber);
		remoteReadLatency = new NumericHistogram(binsNumber);
		remoteWriteLatency = new NumericHistogram(binsNumber);
	}

	int outstanding;

	volatile boolean alive;

	class ZooKeeperThread extends Thread implements Watcher {
		String localZKIDAndHost;
		String remoteZKIDsAndHosts[];

		ZooKeeperThread(String localZKIDAndHost, String remoteZKIDsAndHosts[]) {
			System.out.println("Starting ZK thread with localZKIDAndHost:"+localZKIDAndHost +" remoteZKIDsAndHosts:" +Arrays.toString(remoteZKIDsAndHosts));
			setDaemon(true);
			alive = true;
			this.localZKIDAndHost= localZKIDAndHost;
			this.remoteZKIDsAndHosts= remoteZKIDsAndHosts;
			start();
		}

		synchronized void incOutstanding() throws InterruptedException {
			outstanding++;
			while (outstanding >= outstandingLimit) {
				wait();
			}
		}

		synchronized void decOutstanding() {
			outstanding--;
			notifyAll();
		}

		Random r = new Random();

		String localZKIDAndPath;
		String[] remoteZKIDAndPaths;


		ZooNet zk;

		boolean connected;

		public String createPathInZK(String zkid) throws InterruptedException{
			String path = null;
			System.err.println("Creating path in "+zkid);
			while (path == null) {
				try {
					Thread.sleep(150 + r.nextInt(150));
					path = zk.create(zkid+"/client", new byte[16],
							Ids.OPEN_ACL_UNSAFE,
							CreateMode.EPHEMERAL_SEQUENTIAL);
				} catch (KeeperException e) {
					LOG.error("keeper exception thrown", e);
				}
			}
			System.err.println("Returning path "+path);
			return path;
		}

		public void run() {
			try {
				LOG.info("Client of app "+appName + " connecting to ZKs at "+localZKIDAndHost + " and remote "+Arrays.toString(remoteZKIDsAndHosts));

				// First setting the local ZK
				zk = new ZooNet(localZKIDAndHost, 60000, this, false);
				localZKIDAndPath = createPathInZK("");
				
				// Now the remote ZKs
				if (remoteZKIDsAndHosts.length > 0){
					remoteZKIDAndPaths = new String[remoteZKIDsAndHosts.length];
				}
				for (int i=0; i<remoteZKIDsAndHosts.length; i++){
					String zkid = remoteZKIDsAndHosts[i].substring(0,remoteZKIDsAndHosts[i].indexOf('~'));
					String host = remoteZKIDsAndHosts[i].substring(remoteZKIDsAndHosts[i].indexOf('~')+1); 
					zk.connect(zkid, host, 60000, this);
					synchronized (this) {
						while (!connected) {
							wait(500);
						}
					}
					System.out.println("Connected CaaS ZK in ZKID:"+zkid + " host:" + host);
					connected = false;
					remoteZKIDAndPaths[i] = zkid+"~"+createPathInZK(zkid+"~");
				}
				while (localPercent == -1 || readsPercent == -1) {}
				while (alive) {
					if (r.nextInt(100) < localPercent){ 
						perfromBurstActions(localZKIDAndPath, true);
					} else {
						perfromBurstActions( remoteZKIDAndPaths[r.nextInt(remoteZKIDAndPaths.length)], false);
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				alive = false;
				try {
					for (String zkidAndHost : remoteZKIDsAndHosts){
						zk.close(zkidAndHost.substring(0,zkidAndHost.indexOf('~')));
					}
					zk.close();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}

		private void perfromBurstActions(String ZKForBurst, final boolean isLocalZK) throws InterruptedException {
			for(int i=0; i<burstSize; i++){
				if (r.nextInt(100) < readsPercent) {
					zk.getData(ZKForBurst, false, 
							new DataCallback() {
						@Override
						public void processResult(int arg0, String arg1, Object arg2, byte[] arg3, Stat arg4) {
							ZooKeeperThread.this.processResult(isLocalZK, arg0, arg1, arg2, arg3, arg4);
						}
					}, 
							System.currentTimeMillis());
					incOutstanding();
				} else {
					zk.setData(ZKForBurst, bytes, -1, 
							new StatCallback() {
						@Override
						public void processResult(int arg0, String arg1, Object arg2, Stat arg3) {
							ZooKeeperThread.this.processResult(isLocalZK, arg0, arg1, arg2, arg3);
						}
					}
					, System.currentTimeMillis());
					incOutstanding();
				}
			}
		}

		public void process(WatchedEvent event) {
			System.err.println(event);
			synchronized (this) {
				if (event.getType() == EventType.None) {
					connected = (event.getState() == KeeperState.SyncConnected);
					notifyAll();
				}
			}
		}

		String lostConnectionTo = null;
		public void processResult(boolean isLocal, int rc, String path, Object ctx, byte[] data, Stat stat
				) {
			decOutstanding();
			synchronized (statSync) {
				// Doesn't count requests that belong to a previous test
				if (!alive || lastUpdate > ((long) ctx)) {
					return;
				}
				if (rc != 0) {
					if (rc == KeeperException.Code.CONNECTIONLOSS.intValue()){
						System.err.println("Connection lost");
						return;
					}
					if (isLocal){ 
						System.err.println(
								"Local session "+ Long.toHexString(zk.getSessionId()) +
								" got rc(" + rc + ") when performing read. Latency of error is "+ (System.currentTimeMillis() - ((long) ctx)));
					} else {
						System.err.println(
								"Remote session got rc(" + rc + ") when performing read. Latency of error is "+ (System.currentTimeMillis() - ((long) ctx)));
					}
					errors++;
				} else {
					int delta = (int)(System.currentTimeMillis() - (long)ctx);
					if (isLocal){
						localReadFinished++;
						localReadLatency.add(delta);
					} else {
						remoteReadFinished++;
						remoteReadLatency.add(delta);
					}
				}
			}
		}

		public void processResult(boolean isLocal, int rc, String path, Object ctx, Stat stat) {
			decOutstanding();
			synchronized (statSync) {
				// Doesn't count requests that belong to a previous test
				if (!alive || lastUpdate > ((long) ctx)) {
					return;
				}
				if (rc != 0) {
					if (rc == KeeperException.Code.CONNECTIONLOSS.intValue()){
						System.err.println("Connection lost");
						return;
					}
					if (isLocal){ 
						System.err.println(
								"Local session "+ Long.toHexString(zk.getSessionId()) +
								" got rc(" + rc + ") when performing write. Latency of error is "+ (System.currentTimeMillis() - ((long) ctx)));
					} else {
						System.err.println(
								"Remote session got rc(" + rc + ") when performing write. Latency of error is "+ (System.currentTimeMillis() - ((long) ctx)));
					}
					errors++;
				} else {
					int delta = (int)(System.currentTimeMillis() - (long)ctx);
					if (isLocal){
						localWriteFinished++;
						localWriteLatency.add(delta);
					} else {
						remoteWriteFinished++;
						remoteWriteLatency.add(delta);
					}
				}
			}
		}
	}

	class SenderThread extends Thread {
		Socket s;

		SenderThread(Socket s) {
			this.s = s;
			setDaemon(true);
			start();
		}

		public void run() {
			try {
				OutputStream os = s.getOutputStream();
				resetTestCounters();
				while (alive) {
					Thread.sleep(300);
					if (readsPercent == -1 || localPercent == -1 || 
							(localReadFinished == 0 && localWriteFinished ==0 && remoteReadFinished == 0 && remoteWriteFinished == 0 && errors == 0)) {
						continue;
					}
					String report = 
							appName + " " 
									+ System.currentTimeMillis() + " "
									+ errors + " "
									+ localReadFinished + " " +localWriteFinished + " " + remoteReadFinished + " " +remoteWriteFinished + " "
									+ localReadLatency.serialize() + " "
									+ localWriteLatency.serialize() + " "
									+ remoteReadLatency.serialize() + " "
									+ remoteWriteLatency.serialize() + " "
									+ "\n";
					//                    System.out.println("Reporting: "+report);
					synchronized (statSync) {
						resetTestCounters();
					}
					os.write(report.getBytes());
				}
			} catch (Exception e) {
				e.printStackTrace();
			}

		}
	}

	Socket s;
	ZooKeeperThread zkThread;
	SenderThread sendThread;
	Reporter r;

	public void configure(final String params) {
		System.err.println("Got " + params);
		String localConnectionString = null;
		String[] remoteConnectionStrings = new String[0];
		String hostPort[] = null;
		try {
			String parts[] = params.split(" ");
			String[] localAndRemotes = parts[0].split("\\|");
			localConnectionString = localAndRemotes[0];
			if (localAndRemotes.length > 1){
				remoteConnectionStrings =localAndRemotes[1].split("\\;");
			}
			String[] parameters = parts[1].split("\\|");
			readsPercent = Integer.parseInt(parameters[0]);
			localPercent = Integer.parseInt(parameters[1]);
			bytes = new byte[Integer.parseInt(parts[2])];
			appName = parts[3];
			hostPort = parts[4].split("\\:");
			burstSize = Integer.parseInt(parts[5]);
			outstandingLimit = Integer.parseInt(parts[6]);
			binsNumber = Integer.parseInt(parts[7]);
		} catch (Exception e) {
			LOG.error("configure");
			e.printStackTrace();
			return;
		}
		try {
			s = new Socket(hostPort[0], Integer.parseInt(hostPort[1]));
			zkThread = new ZooKeeperThread(localConnectionString, remoteConnectionStrings);
			sendThread = new SenderThread(s);
		} 
		catch(SocketException e){
			LOG.error(e.getMessage()+" terminated");
			return;
		}
		catch (Exception e) {
			LOG.error("terminated");
			e.printStackTrace();
		} 
	}

	public void setReporter(Reporter r) {
		this.r = r;
	}

	public void start() {
		try {
			r.report("started");
		} catch (Exception e) {
			LOG.error(e.getMessage()+ " Reported start");
			e.printStackTrace();
		}
	}

	public void stop() {
		alive = false;
		zkThread.interrupt();
		sendThread.interrupt();
		try {
			zkThread.join();
		} catch (InterruptedException e) {
			LOG.error(e.getMessage()+ " Reported stop 1");
			e.printStackTrace();
		}
		try {
			sendThread.join();
		} catch (InterruptedException e) {
			LOG.error(e.getMessage()+ " Reported stop 2");
			e.printStackTrace();
		}
		try {
			r.report("stopped");
		} catch (Exception e) {
			LOG.error(e.getMessage()+ " Reported stop 3");
			e.printStackTrace();
		}
		try {
			s.close();
		} catch (IOException e) {
			LOG.error(e.getMessage()+ " Reported stop 4");
			e.printStackTrace();
		}
	}

}