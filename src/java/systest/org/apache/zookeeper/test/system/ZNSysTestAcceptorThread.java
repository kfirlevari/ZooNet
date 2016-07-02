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
import java.net.Socket;
import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ZNSysTestAcceptorThread extends Thread {
    private static final Logger LOG = LoggerFactory.getLogger(ZNSysTestAcceptorThread.class);

    ZNSysTestAcceptorThread() {
        setDaemon(true);
        start();
    }

    public void run() {
        try {
            while (true) {
                Socket s = ZNSysTest.ss.accept();
                System.err.println("Accepted connection from " + s);
                ZNSysTest.slaves.add(new ZNSysTestSlaveThread(s));
            }
        } catch (IOException e) {
            LOG.error("AcceptorThread");
            e.printStackTrace();
        } finally {
            for (Iterator<ZNSysTestSlaveThread> it = ZNSysTest.slaves.iterator(); it.hasNext();) {
                ZNSysTestSlaveThread st = it.next();
                it.remove();
                st.close();
            }
        }
    }
}