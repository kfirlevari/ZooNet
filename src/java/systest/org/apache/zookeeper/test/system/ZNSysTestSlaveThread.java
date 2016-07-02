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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ZNSysTestSlaveThread extends Thread {
    protected static final Logger LOG = LoggerFactory.getLogger(ZNSysTestSlaveThread.class);

    Socket s;

    ZNSysTestSlaveThread(Socket s) {
        setDaemon(true);
        this.s = s;
        start();
    }

    public void run() {
        try {
            System.out.println("Connected to " + s);
            BufferedReader is = new BufferedReader(new InputStreamReader(s
                    .getInputStream()));
            String result;
            // result = <appName> <time> <errs> 
            // <local read count> <local write count> <remote read count> <remote write count>
            // <local read latency> <local write latency> <remote read latency> <remote write latency>
            while ((result = is.readLine()) != null) {
                String clientInfo[] = result.split(" ");
                String appName = clientInfo[0];
                long time = Long.parseLong(clientInfo[1]);
                int errs = Integer.parseInt(clientInfo[2]); 
                int countLocalReads = Integer.parseInt(clientInfo[3]);
                int countLocalWrites = Integer.parseInt(clientInfo[4]);
                int countRemoteReads = Integer.parseInt(clientInfo[5]);
                int countRemoteWrites = Integer.parseInt(clientInfo[6]);

                int[] latencyLocalReads = NumericHistogram.sendableHistogramToIntArr(clientInfo[7]);
                int[] latencyLocalWrites = NumericHistogram.sendableHistogramToIntArr(clientInfo[8]);
                int[] latencyRemoteReads = NumericHistogram.sendableHistogramToIntArr(clientInfo[9]);
                int[] latencyRemoteWrites = NumericHistogram.sendableHistogramToIntArr(clientInfo[10]);
        		
                if (errs > 0) {
                    System.out.println(s + " Got an error! " + errs);
                }
                ZNSysTest.add(appName, time, countLocalReads, countLocalWrites, countRemoteReads, countRemoteWrites,
                		latencyLocalReads, latencyLocalWrites, latencyRemoteReads, latencyRemoteWrites,
                		s);
            }
        } catch (Exception e) {
            LOG.error("SlaveThread");
            e.printStackTrace();
        } finally {
            close();
        }
    }

    void sendMsg(String msg){
        try {
            s.getOutputStream().write((msg + "\n").getBytes());
        } catch (IOException e) {
            LOG.error("SlaveThread sendMsg");
            e.printStackTrace();
        }
    }

    void close() {
        try {
            System.err.println("Closing " + s);
            ZNSysTest.slaves.remove(this);
            s.close();
        } catch (IOException e) {
            LOG.error("SlaveThread close");
            e.printStackTrace();
        }
    }
}