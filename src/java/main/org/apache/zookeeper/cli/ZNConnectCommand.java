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
package org.apache.zookeeper.cli;

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.Parser;
import org.apache.commons.cli.PosixParser;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ZooNet connect to a remote ZK
 */
public class ZNConnectCommand extends CliCommand {

    private static final Logger LOG = LoggerFactory.getLogger(ZNConnectCommand.class);
    private static Options options = new Options();
    private String args[];
    private CommandLine cl;

    public ZNConnectCommand() {
        super("znConnect", "remoteZKID connectString");
    }
    
    private class MyRemoteZKWatcher implements Watcher {
        public void process(WatchedEvent event) {
        	LOG.info("WATCHER::" + event.toString());
        }
    }

    @Override
    public CliCommand parse(String[] cmdArgs) throws ParseException {

        Parser parser = new PosixParser();
        cl = parser.parse(options, cmdArgs);
        args = cl.getArgs();
        if (args.length < 2) {
            throw new ParseException(getUsageStr());
        }
        return this;
    }

    @Override
    public boolean exec() throws KeeperException, InterruptedException {
        String remoteZKID = args[1];
        String connectString = args[2];
        try {
			zk.connect(remoteZKID, connectString, zk.getSessionTimeout(), new MyRemoteZKWatcher());
		} catch (IOException e) {
			e.printStackTrace();
		}
        
        return false;
    }
}
