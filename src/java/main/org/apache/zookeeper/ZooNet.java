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

package org.apache.zookeeper;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

import org.apache.zookeeper.AsyncCallback.ACLCallback;
import org.apache.zookeeper.AsyncCallback.Children2Callback;
import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.AsyncCallback.Create2Callback;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.AsyncCallback.MultiCallback;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.AsyncCallback.VoidCallback;
import org.apache.zookeeper.Watcher.WatcherType;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.client.ZKClientConfig;
import org.apache.zookeeper.client.ZooKeeperSaslClient;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

/**
 * ZooNet wraps ZooKeeper actions so that every action is considered as a local action, 
 * unless its path starts with "ZKID~\znode1\...", in that case, ZKID is removed from the path
 * and the action is performed on the suitable remote ZooKeeper. 
 */
public class ZooNet extends ZooKeeper {
	private HashMap<String, ZooKeeper> remoteZKIDToConnection = new HashMap<String,ZooKeeper>();
	final static String LOCAL_ZKID = "LOCAL";
	private final OutstandingCounter outstanding = new OutstandingCounter();
	public class OutstandingCounter {
	    private int numOutstanding = 0;

	    public synchronized void increase() {
	        this.numOutstanding++;
	    }

	    public synchronized void decrease() {
	        this.numOutstanding--;
	        if (this.numOutstanding == 0){
	            this.notify();
	        }
	    }
	    
	    public synchronized void waitForNoOutstanding(){
            try {
                while(this.numOutstanding != 0) wait();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
	    }
	  }
	
	private final static String SYNC_NODE = "/zookeeper";
	
    /**
     * Use this constructor for the local ZK connection.
     * @param localZKConnectString
     * @param sessionTimeout
     * @param watcher
     * @param canBeReadOnly
     * @throws IOException
     */
    public ZooNet(String localZKConnectString, int sessionTimeout, Watcher watcher,
            boolean canBeReadOnly) throws IOException {
        super(localZKConnectString, sessionTimeout, watcher, canBeReadOnly);
        this.remoteZKIDToConnection.put(LOCAL_ZKID, super.getClientRef()); 
    }
    
	// Currently, ZK.sync() doesn't answer our needs for linearizable updates. 
	// So we perform sync using set data to unused znode
    private void sync(ZooKeeper zooKeeper) {
        outstanding.increase();
		zooKeeper.setData(SYNC_NODE, new byte[1], -1, new StatCallback() { 
		    public void processResult(int rc, String path, Object ctx, Stat stat) {
		    outstanding.decrease();
		}}, null);
	}

    /**
     * Creates a new remote ZooKeeper connection. 
     * The application needs to pass a zkid that will be used as a prefix "(zkid~)" in all 
     * requests to this zookeeper connection. 
     * @see ZooKeeper#Connect(String, int, Watcher)
     */
	public void connect(String zkid, String connectString, int sessionTimeout, Watcher watcher) throws IOException {
		this.remoteZKIDToConnection.put(zkid, new ZooKeeper(connectString, sessionTimeout, watcher)); 
	}
	
    public void connect(String zkid, String connectString, int sessionTimeout, Watcher watcher, boolean readOnly) throws IOException {
        this.remoteZKIDToConnection.put(zkid, new ZooKeeper(connectString, sessionTimeout, watcher, readOnly)); 
    }

    /**
     * Close remote zkid's client object. 
     * @see ZooKeeper#close(String)
     */
    public void close(String remoteZKID) throws InterruptedException {
    	this.remoteZKIDToConnection.get(remoteZKID).close();
    }
    
    private class OpSharedParams {
        ZooKeeper zk;
        String path;
    }
    private String lastZKID = LOCAL_ZKID;
    private OpSharedParams addSyncIfNeededBeforeRead(String path){
        OpSharedParams params = new OpSharedParams();
        if (path.charAt(0) != '/'){     
            // A remote action
            String remoteZKID = path.substring(0, path.indexOf('~'));
            params.zk = this.remoteZKIDToConnection.get(remoteZKID);
            params.path = path.substring(path.indexOf('/'));
            if (!remoteZKID.equals(lastZKID)){
                outstanding.waitForNoOutstanding();
                sync(params.zk);
            }
            lastZKID = remoteZKID;
        } else {
            params.zk = this.remoteZKIDToConnection.get(LOCAL_ZKID);
            params.path = path;
            // A local action
            if (!lastZKID.equals(LOCAL_ZKID)){
                outstanding.waitForNoOutstanding();
                // We need to sync only when we start reading from a different ZK
                sync(params.zk);
            }
            lastZKID = LOCAL_ZKID;
        }
        return params;
    }
    
    /**
     * Perform sync based on ZKID (for read operations that are path-less).
     * @param zkid empty means local ZKID.
     * @return
     */
    private OpSharedParams addSyncIfNeededBeforeReadByZKID(String zkid){
        OpSharedParams params = new OpSharedParams();
        if (!zkid.isEmpty()){     
            // A remote action
            params.zk = this.remoteZKIDToConnection.get(zkid);
            if (!zkid.equals(lastZKID)){
                outstanding.waitForNoOutstanding();
                sync(params.zk);
            }
            lastZKID = zkid;
        } else {
            params.zk = this.remoteZKIDToConnection.get(LOCAL_ZKID);
            // A local action
            if (!lastZKID.equals(LOCAL_ZKID)){
                outstanding.waitForNoOutstanding();
                // We need to sync only when we start reading from a different ZK
                sync(params.zk);
            }
            lastZKID = LOCAL_ZKID;
        }
        return params;
    }
    
    private OpSharedParams getZKByZKID(String zkid){
        OpSharedParams params = new OpSharedParams();
        if (!zkid.isEmpty()){     
            params.zk = this.remoteZKIDToConnection.get(zkid);
        } else {
            params.zk = this.remoteZKIDToConnection.get(LOCAL_ZKID);
        }
        return params;
    }
    
    private OpSharedParams setLastZKIDBeforeUpdate(String path){
        OpSharedParams params = new OpSharedParams();
        String newZKID = null;
        if (path.charAt(0) != '/'){
            newZKID = path.substring(0, path.indexOf('~'));
            params.path = path.substring(path.indexOf('/'));
            params.zk = this.remoteZKIDToConnection.get(lastZKID);
        } else {
            newZKID = LOCAL_ZKID;
            params.zk = this.remoteZKIDToConnection.get(LOCAL_ZKID);
            params.path = path;
        }
        if (!lastZKID.equals(newZKID)){
            outstanding.waitForNoOutstanding();
        }
        lastZKID = newZKID;
        return params;
    }
    
    /**
     * Updates last ZKID (for update operations that are path-less).
     * @param zkid empty means local ZKID.
     * @return
     */
    private OpSharedParams setLastZKIDBeforeUpdateByZKID(String zkid){
        OpSharedParams params = new OpSharedParams();
        String newZKID = null;
        if (!zkid.isEmpty()){
            newZKID = zkid;
            params.zk = this.remoteZKIDToConnection.get(lastZKID);
        } else {
            newZKID = LOCAL_ZKID;
            params.zk = this.remoteZKIDToConnection.get(LOCAL_ZKID);
        }        
        if (!lastZKID.equals(newZKID)){
            outstanding.waitForNoOutstanding();
        }
        lastZKID = newZKID;
        return params;
    }
    
    /**
     * 
     * 
     * Below this point, we simply wrap ZooKeeper's functions with ZN layer.
     * 
     * 
     */
    
    /**
     * @param path the given path, if remote action then starts with zkid of the relevant remote zookeeper. I.e., ZK1~/znode/...
     * @see org.apache.zookeeper.ZooKeeper#setData(String, byte[], int)
     */
    public Stat setData(final String path, byte data[], int version)
            throws KeeperException, InterruptedException{
        OpSharedParams params = setLastZKIDBeforeUpdate(path);
        return params.zk.setData(params.path, data, version);
    }
    
    /**
     * @param path the given path, if remote action then starts with zkid of the relevant remote zookeeper. I.e., ZK1~/znode/...
     * @see org.apache.zookeeper.ZooKeeper#setData(String, byte[], int, StatCallback, Object)
     */
    public void setData(final String path, byte data[], int version,
            final StatCallback cb, Object ctx) {
        OpSharedParams params = setLastZKIDBeforeUpdate(path);
        outstanding.increase();
        params.zk.setData(params.path, data, version, 
        new StatCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx, Stat stat) {
                outstanding.decrease();
                cb.processResult(rc, path, ctx, stat);
            }
        }
        , ctx);
    }
    
    /**
     * @param zkid of the ZK on which updateServerList is called, empty for local zk
     * @see org.apache.zookeeper.ZooKeeper#updateServerList(String)
     */
    public void updateServerList(String zkid, String connectString) throws IOException {
        OpSharedParams params = getZKByZKID(zkid);
        params.zk.updateServerList(connectString);
    }

    /**
     * @param zkid of the ZK on which getSaslClient is called, empty for local zk
     * @see org.apache.zookeeper.ZooKeeper#getSaslClient()
     */
    public ZooKeeperSaslClient getSaslClient(String zkid) {
        OpSharedParams params = getZKByZKID(zkid);
        return params.zk.getSaslClient();
    }

    /**
     * @param zkid of the ZK on which getClientConfig is called, empty for local zk
     * @see org.apache.zookeeper.ZooKeeper#getClientConfig()
     */
    public ZKClientConfig getClientConfig(String zkid) {
        OpSharedParams params = getZKByZKID(zkid);
        return params.zk.getClientConfig();
    }

    /**
     * @param zkid of the ZK on which getDataWatches is called, empty for local zk
     * @see org.apache.zookeeper.ZooKeeper#getDataWatches()
     */
    List<String> getDataWatches(String zkid) {
        OpSharedParams params = getZKByZKID(zkid);
        return params.zk.getDataWatches();
    }
    
    /**
     * @param zkid of the ZK on which getExistWatches is called, empty for local zk
     * @see org.apache.zookeeper.ZooKeeper#getExistWatches()
     */
    List<String> getExistWatches(String zkid) {
        OpSharedParams params = getZKByZKID(zkid);
        return params.zk.getExistWatches();
    }
    
    /**
     * @param zkid of the ZK on which getChildWatches is called, empty for local zk
     * @see org.apache.zookeeper.ZooKeeper#getChildWatches()
     */
    List<String> getChildWatches(String zkid) {
        OpSharedParams params = getZKByZKID(zkid);
        return params.zk.getChildWatches();
    }


    /**
     * @param zkid of the ZK on which getSessionId is called, empty for local zk
     * @see org.apache.zookeeper.ZooKeeper#getSessionId()
     */
    public long getSessionId(String zkid) {
        OpSharedParams params = getZKByZKID(zkid);
        return params.zk.getSessionId();
    }

    /**
     * @param zkid of the ZK on which getSessionPasswd is called, empty for local zk
     * @see org.apache.zookeeper.ZooKeeper#getSessionPasswd()
     */
    public byte[] getSessionPasswd(String zkid) {
        OpSharedParams params = getZKByZKID(zkid);
        return params.zk.getSessionPasswd();
    }

    /**
     * @param zkid of the ZK on which getSessionTimeout is called, empty for local zk
     * @see org.apache.zookeeper.ZooKeeper#getSessionTimeout()
     */
    public int getSessionTimeout(String zkid) {
        OpSharedParams params = getZKByZKID(zkid);
        return params.zk.getSessionTimeout();
    }

    /**
     * @param zkid of the ZK on which addAuthInfo is called, empty for local zk
     * @see org.apache.zookeeper.ZooKeeper#addAuthInfo(String, byte[])
     */
    public void addAuthInfo(String zkid, String scheme, byte auth[]) {
        OpSharedParams params = getZKByZKID(zkid);
        params.zk.addAuthInfo(scheme, auth);
    }

    /**
     * @param zkid of the ZK on which register is called, empty for local zk
     * @see org.apache.zookeeper.ZooKeeper#register(Watcher)
     */
    public synchronized void register(String zkid, Watcher watcher) {
        OpSharedParams params = getZKByZKID(zkid);
        params.zk.register(watcher);
    }

    /**
     * @param path the given path, if remote action then starts with zkid of the relevant remote zookeeper. I.e., ZK1~/znode/...
     * @see org.apache.zookeeper.ZooKeeper#create(String, byte[], List, CreateMode) 
     */
    public String create(final String path, byte data[], List acl, CreateMode createMode)
        throws KeeperException, InterruptedException{
        OpSharedParams params = setLastZKIDBeforeUpdate(path);
        return params.zk.create(params.path, data, acl, createMode);
    }
    
    /**
     * @param path the given path, if remote action then starts with zkid of the relevant remote zookeeper. I.e., ZK1~/znode/...
     * @see org.apache.zookeeper.ZooKeeper#create(String, byte[], List, CreateMode, Stat) 
     */
    public String create(final String path, byte data[], List acl,
            CreateMode createMode, Stat stat)
            throws KeeperException, InterruptedException {
        OpSharedParams params = setLastZKIDBeforeUpdate(path);
        return params.zk.create(params.path, data, acl, createMode, stat);
    }

    /**
     * @param path the given path, if remote action then starts with zkid of the relevant remote zookeeper. I.e., ZK1~/znode/...
     * @see org.apache.zookeeper.ZooKeeper#create(String, byte[], List, CreateMode, StringCallback, Object) 
     */
    public void create(final String path, byte data[], List acl,
            CreateMode createMode, final StringCallback cb, Object ctx)
    {
        OpSharedParams params = setLastZKIDBeforeUpdate(path);
        outstanding.increase();
        params.zk.create(params.path, data, acl, createMode, 
        new StringCallback() {
            
            @Override
            public void processResult(int rc, String path, Object ctx, String name) {
                outstanding.decrease();
                cb.processResult(rc, path, ctx, name);
            }
        }, ctx);
    }

    /**
     * @param path the given path, if remote action then starts with zkid of the relevant remote zookeeper. I.e., ZK1~/znode/...
     * @see org.apache.zookeeper.ZooKeeper#create(String, byte[], List, CreateMode, Create2Callback, Object) 
     */
    public void create(final String path, byte data[], List acl,
            CreateMode createMode, final Create2Callback cb, Object ctx)
    {
        OpSharedParams params = setLastZKIDBeforeUpdate(path);
        outstanding.increase();
        params.zk.create(params.path, data, acl, createMode, 
        new Create2Callback() {
            
            @Override
            public void processResult(int rc, String path, Object ctx, String name, Stat stat) {
                outstanding.decrease();
                cb.processResult(rc, path, ctx, name, stat); 
            }
        }, ctx);
    }

    /**
     * @param path the given path, if remote action then starts with zkid of the relevant remote zookeeper. I.e., ZK1~/znode/...
     * @see org.apache.zookeeper.ZooKeeper#delete(String, int) 
     */
    public void delete(final String path, int version)
        throws InterruptedException, KeeperException{
        OpSharedParams params = setLastZKIDBeforeUpdate(path);
        params.zk.delete(params.path, version);
    }

    /**
     * @param path the given path, if remote action then starts with zkid of the relevant remote zookeeper. I.e., ZK1~/znode/...
     * @see org.apache.zookeeper.ZooKeeper#delete(String, int) 
     */
    public void delete(final String path, int version, final VoidCallback cb,
            Object ctx){
        OpSharedParams params = setLastZKIDBeforeUpdate(path);
        outstanding.increase();
        params.zk.delete(params.path, version, new VoidCallback() {
            
            @Override
            public void processResult(int rc, String path, Object ctx) {
                outstanding.decrease();
                cb.processResult(rc, path, ctx);
            }
        }, ctx);
    }
    
    public boolean isReadOp(int type) {
        switch (type) {
        case OpCode.exists:
        case OpCode.getACL:
        case OpCode.getChildren:
        case OpCode.getChildren2:
        case OpCode.getData:
            return true;
        case OpCode.create:
        case OpCode.create2:
        case OpCode.createContainer:
        case OpCode.error:
        case OpCode.delete:
        case OpCode.deleteContainer:
        case OpCode.setACL:
        case OpCode.setData:
        case OpCode.check:
        case OpCode.multi:
        case OpCode.reconfig:
            return false;
        default:
            return true;
        }
    }
    
    /**
     * @param zkid of the ZK on which multi is called, empty for local zk
     * @see org.apache.zookeeper.ZooKeeper#multi(Iterable)
     */
    public List<OpResult> multi(String zkid, Iterable<Op> ops) throws InterruptedException, KeeperException {
        if (isReadOp(ops.iterator().next().getType())){
            OpSharedParams params = addSyncIfNeededBeforeReadByZKID(zkid);
            return params.zk.multi(ops);
        }
        
        OpSharedParams params = setLastZKIDBeforeUpdateByZKID(zkid);
        return params.zk.multi(ops);
    }

    /**
     * @param zkid of the ZK on which multi is called, empty for local zk
     * @see org.apache.zookeeper.ZooKeeper#multi(Iterable, MultiCallback, Object)
     */
    public void multi(String zkid, Iterable<Op> ops, final MultiCallback cb, Object ctx) {
        if (isReadOp(ops.iterator().next().getType())){
            OpSharedParams params = addSyncIfNeededBeforeReadByZKID(zkid);
            outstanding.increase();
            params.zk.multi(ops, new MultiCallback() {
                
                @Override
                public void processResult(int rc, String path, Object ctx, List opResults) {
                    outstanding.decrease();
                    cb.processResult(rc, path, ctx, opResults);
                }
            }, ctx);
            return;
        }
        
        OpSharedParams params = setLastZKIDBeforeUpdateByZKID(zkid);
        outstanding.increase();
        params.zk.multi(ops, new MultiCallback() {
            
            @Override
            public void processResult(int rc, String path, Object ctx, List opResults) {
                outstanding.decrease();
                cb.processResult(rc, path, ctx, opResults);
            }
        }, ctx);
    }

    /**
     * @param path the given path, if remote action then starts with zkid of the relevant remote zookeeper. I.e., ZK1~/znode/...
     * @see org.apache.zookeeper.ZooKeeper#exists(String, Watcher)
     */
    public Stat exists(final String path, Watcher watcher)
        throws KeeperException, InterruptedException{        
        OpSharedParams params = addSyncIfNeededBeforeRead(path);
        return params.zk.exists(params.path, watcher);
    }

    /**
     * @param path the given path, if remote action then starts with zkid of the relevant remote zookeeper. I.e., ZK1~/znode/...
     * @see org.apache.zookeeper.ZooKeeper#exists(String, boolean)
     */
    public Stat exists(String path, boolean watch) throws KeeperException,
        InterruptedException{        
        OpSharedParams params = addSyncIfNeededBeforeRead(path);
        return params.zk.exists(params.path, watch);
    }

    /**
     * @param path the given path, if remote action then starts with zkid of the relevant remote zookeeper. I.e., ZK1~/znode/...
     * @see org.apache.zookeeper.ZooKeeper#exists(String, Watcher, StatCallback, Object)
     */
    public void exists(final String path, Watcher watcher,
            final StatCallback cb, Object ctx){
        OpSharedParams params = addSyncIfNeededBeforeRead(path);
        outstanding.increase();
        params.zk.exists(params.path, watcher, 
                new StatCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx, Stat stat) {
                outstanding.decrease();
                cb.processResult(rc, path, ctx, stat);
            }
        }
        , ctx);
    }

    /**
     * @param path the given path, if remote action then starts with zkid of the relevant remote zookeeper. I.e., ZK1~/znode/...
     * @see org.apache.zookeeper.ZooKeeper#exists(String, boolean, StatCallback, Object)
     */
    public void exists(String path, boolean watch, final StatCallback cb, Object ctx) {
        OpSharedParams params = addSyncIfNeededBeforeRead(path);
        outstanding.increase();
        params.zk.exists(params.path, watch,
        new StatCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx, Stat stat) {
                outstanding.decrease();
                cb.processResult(rc, path, ctx, stat);
            }
        }
        , ctx);
    }

    /**
     * @param path the given path, if remote action then starts with zkid of the relevant remote zookeeper. I.e., ZK1~/znode/...
     * @see org.apache.zookeeper.ZooKeeper#getData(String, Watcher, Stat)
     */
    public byte[] getData(final String path, Watcher watcher, Stat stat)
        throws KeeperException, InterruptedException {
        OpSharedParams params = addSyncIfNeededBeforeRead(path);
        return params.zk.getData(params.path, watcher, stat);
    }

    /**
     * @param path the given path, if remote action then starts with zkid of the relevant remote zookeeper. I.e., ZK1~/znode/...
     * @see org.apache.zookeeper.ZooKeeper#getData(String, Watcher, DataCallback, Object)
     */
    public void getData(final String path, Watcher watcher,
            final DataCallback cb, Object ctx){
        OpSharedParams params = addSyncIfNeededBeforeRead(path);
        outstanding.increase();
        params.zk.getData(params.path, watcher, 
                new DataCallback() {
                    
                    @Override
                    public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
                        outstanding.decrease();
                        cb.processResult(rc, path, ctx, data, stat);
                    }
                }, ctx);
    }

    /**
     * @param path the given path, if remote action then starts with zkid of the relevant remote zookeeper. I.e., ZK1~/znode/...
     * @see org.apache.zookeeper.ZooKeeper#getData(String, boolean, Stat)
     */
    public byte[] getData(String path, boolean watch, Stat stat) throws KeeperException, InterruptedException{
        OpSharedParams params = addSyncIfNeededBeforeRead(path);
        return params.zk.getData(params.path, watch, stat);
    }

    /**
     * @param path the given path, if remote action then starts with zkid of the relevant remote zookeeper. I.e., ZK1~/znode/...
     * @see org.apache.zookeeper.ZooKeeper#getData(String, boolean, DataCallback, Object)
     */
    public void getData(String path, boolean watch, final DataCallback cb, Object ctx) {
        OpSharedParams params = addSyncIfNeededBeforeRead(path);
        outstanding.increase();
        params.zk.getData(params.path, watch, 
                new DataCallback() {
                    @Override
                    public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
                        outstanding.decrease();
                        cb.processResult(rc, path, ctx, data, stat);
                    }
                }, ctx);
    }

    /**
     * @param zkid of the ZK on which getConfig is called, empty for local zk
     * @see org.apache.zookeeper.ZooKeeper#getConfig(Watcher, Stat)
     */
    public byte[] getConfig(String zkid, Watcher watcher, Stat stat)
        throws KeeperException, InterruptedException {
        OpSharedParams params = addSyncIfNeededBeforeReadByZKID(zkid);
        return params.zk.getConfig(watcher, stat);
    }

    /**
     * @param zkid of the ZK on which getConfig is called, empty for local zk
     * @see org.apache.zookeeper.ZooKeeper#getConfig(Watcher, DataCallback, Object)
     */
    public void getConfig(String zkid,  Watcher watcher,
            final DataCallback cb, Object ctx) {
        OpSharedParams params = addSyncIfNeededBeforeReadByZKID(zkid);
        outstanding.increase();
        params.zk.getConfig(watcher, new DataCallback() {
            
            @Override
            public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
                outstanding.decrease();
                cb.processResult(rc, path, ctx, data, stat);
            }
        }, ctx);
    }

    /**
     * @param zkid of the ZK on which getConfig is called, empty for local zk
     * @see org.apache.zookeeper.ZooKeeper#getConfig(boolean, Stat)
     */
    public byte[] getConfig(String zkid, boolean watch, Stat stat)
            throws KeeperException, InterruptedException {
        OpSharedParams params = addSyncIfNeededBeforeReadByZKID(zkid);
        return params.zk.getConfig(watch, stat);
    }
 
    /**
     * @param zkid of the ZK on which getConfig is called, empty for local zk
     * @see org.apache.zookeeper.ZooKeeper#getConfig(boolean, DataCallback, Object)
     */
    public void getConfig(String zkid, boolean watch, final DataCallback cb, Object ctx) {
        OpSharedParams params = addSyncIfNeededBeforeReadByZKID(zkid);
        outstanding.increase();
        params.zk.getConfig(watch, new DataCallback() {
            
            @Override
            public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
                outstanding.decrease();
                cb.processResult(rc, path, ctx, data, stat);
            }
        }, ctx);
    }
    
    /**
     * @param zkid of the ZK on which reconfig is called, empty for local zk
     * @see org.apache.zookeeper.ZooKeeper#reconfig(String, String, String, long, Stat)
     */
    public byte[] reconfig(String zkid, String joiningServers, String leavingServers, String newMembers, long fromConfig, Stat stat) throws KeeperException, InterruptedException{
        OpSharedParams params = setLastZKIDBeforeUpdateByZKID(zkid);
        return params.zk.reconfig(joiningServers, leavingServers, newMembers, fromConfig, stat);
    }

    /**
     * @param zkid of the ZK on which reconfig is called, empty for local zk
     * @see org.apache.zookeeper.ZooKeeper#reconfig(List, List, List, long, Stat)
     */
    public byte[] reconfig(String zkid, List<String> joiningServers, List<String> leavingServers, List<String> newMembers, long fromConfig, Stat stat) throws KeeperException, InterruptedException{
        OpSharedParams params = setLastZKIDBeforeUpdateByZKID(zkid);
        return params.zk.reconfig(joiningServers, leavingServers, newMembers, fromConfig, stat);
    }

    /**
     * @param zkid of the ZK on which reconfig is called, empty for local zk
     * @see org.apache.zookeeper.ZooKeeper#reconfig(String, String, String, long, DataCallback, Object)
     */
    public void reconfig(String zkid, String joiningServers, String leavingServers,
        String newMembers, long fromConfig, final DataCallback cb, Object ctx){
        OpSharedParams params = setLastZKIDBeforeUpdateByZKID(zkid);
        params.zk.reconfig(joiningServers, leavingServers, newMembers, fromConfig, cb, ctx);
    }
 
    /**
     * @param zkid of the ZK on which reconfig is called, empty for local zk
     * @see org.apache.zookeeper.ZooKeeper#reconfig(List, List, List, long, DataCallback, Object)
     */
    public void reconfig(String zkid, List<String> joiningServers,
        List<String> leavingServers, List<String> newMembers, long fromConfig,
        final DataCallback cb, Object ctx){
        OpSharedParams params = setLastZKIDBeforeUpdateByZKID(zkid);
        outstanding.increase();
        params.zk.reconfig(joiningServers, leavingServers, newMembers, fromConfig, new DataCallback() {
            
            @Override
            public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
                outstanding.decrease();
                cb.processResult(rc, path, ctx, data, stat);
            }
        }, ctx);
    }
   
    /**
     * @param path the given path, if remote action then starts with zkid of the relevant remote zookeeper. I.e., ZK1~/znode/...
     * @see org.apache.zookeeper.ZooKeeper#getACL(String, Stat)
     */
    public List<ACL> getACL(final String path, Stat stat)
        throws KeeperException, InterruptedException {
        OpSharedParams params = addSyncIfNeededBeforeRead(path);
        return params.zk.getACL(params.path, stat);       
    }

    /**
     * @param path the given path, if remote action then starts with zkid of the relevant remote zookeeper. I.e., ZK1~/znode/...
     * @see org.apache.zookeeper.ZooKeeper#getACL(String, Stat, ACLCallback, Object)
     */
    public void getACL(final String path, Stat stat, final ACLCallback cb,
            Object ctx){
        OpSharedParams params = addSyncIfNeededBeforeRead(path);
        outstanding.increase();
        params.zk.getACL(params.path, stat, new ACLCallback() {
            
            @Override
            public void processResult(int rc, String path, Object ctx, List acl, Stat stat) {
                outstanding.decrease();
                cb.processResult(rc, path, ctx, acl, stat);
            }
        }, ctx);  
    }

    /**
     * @param path the given path, if remote action then starts with zkid of the relevant remote zookeeper. I.e., ZK1~/znode/...
     * @see org.apache.zookeeper.ZooKeeper#setACL(String, List, int)
     */
    public Stat setACL(final String path, List acl, int version)
        throws KeeperException, InterruptedException {
        OpSharedParams params = setLastZKIDBeforeUpdate(path);
        return params.zk.setACL(params.path, acl, version);
    }

    /**
     * @param path the given path, if remote action then starts with zkid of the relevant remote zookeeper. I.e., ZK1~/znode/...
     * @see org.apache.zookeeper.ZooKeeper#setACL(String, List, int, StatCallback, Object)
     */
    public void setACL(final String path, List acl, int version,
            final StatCallback cb, Object ctx){
        OpSharedParams params = setLastZKIDBeforeUpdate(path);
        outstanding.increase();
        params.zk.setACL(params.path, acl, version,
        new StatCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx, Stat stat) {
                outstanding.decrease();
                cb.processResult(rc, path, ctx, stat);
            }
        }
        , ctx);
    }

    /**
     * @param path the given path, if remote action then starts with zkid of the relevant remote zookeeper. I.e., ZK1~/znode/...
     * @see org.apache.zookeeper.ZooKeeper#getChildren(String, Watcher)
     */
    public List<String> getChildren(final String path, Watcher watcher)
        throws KeeperException, InterruptedException {
        OpSharedParams params = addSyncIfNeededBeforeRead(path);
        return params.zk.getChildren(params.path, watcher);
    }

    /**
     * @param path the given path, if remote action then starts with zkid of the relevant remote zookeeper. I.e., ZK1~/znode/...
     * @see org.apache.zookeeper.ZooKeeper#getChildren(String, boolean)
     */
    public List<String> getChildren(String path, boolean watch)
            throws KeeperException, InterruptedException {
        OpSharedParams params = addSyncIfNeededBeforeRead(path);
        return params.zk.getChildren(params.path, watch);
    }

    /**
     * @param path the given path, if remote action then starts with zkid of the relevant remote zookeeper. I.e., ZK1~/znode/...
     * @see org.apache.zookeeper.ZooKeeper#getChildren(String, Watcher, ChildrenCallback, Object)
     */
    public void getChildren(final String path, Watcher watcher,
            final ChildrenCallback cb, Object ctx){
        OpSharedParams params = addSyncIfNeededBeforeRead(path);
        outstanding.increase();
        params.zk.getChildren(params.path, watcher, new ChildrenCallback() {
            
            @Override
            public void processResult(int rc, String path, Object ctx, List children) {
                outstanding.decrease();
                cb.processResult(rc, path, ctx, children);
                
            }
        }, ctx);
    }

    /**
     * @param path the given path, if remote action then starts with zkid of the relevant remote zookeeper. I.e., ZK1~/znode/...
     * @see org.apache.zookeeper.ZooKeeper#getChildren(String, boolean, ChildrenCallback, Object)
     */
    public void getChildren(String path, boolean watch, final ChildrenCallback cb,
            Object ctx){
        OpSharedParams params = addSyncIfNeededBeforeRead(path);
        outstanding.increase();
        params.zk.getChildren(params.path, watch, new ChildrenCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx, List children) {
                outstanding.decrease();
                cb.processResult(rc, path, ctx, children);
            }
        }, ctx);
    }

    /**
     * @param path the given path, if remote action then starts with zkid of the relevant remote zookeeper. I.e., ZK1~/znode/...
     * @see org.apache.zookeeper.ZooKeeper#getChildren(String, Watcher, Stat)
     */
    public List<String> getChildren(final String path, Watcher watcher,
            Stat stat)
        throws KeeperException, InterruptedException{
        OpSharedParams params = addSyncIfNeededBeforeRead(path);
        return params.zk.getChildren(params.path, watcher);
    }

    /**
     * @param path the given path, if remote action then starts with zkid of the relevant remote zookeeper. I.e., ZK1~/znode/...
     * @see org.apache.zookeeper.ZooKeeper#getChildren(String, boolean, Stat)
     */
    public List<String> getChildren(String path, boolean watch, Stat stat)
            throws KeeperException, InterruptedException {
        OpSharedParams params = addSyncIfNeededBeforeRead(path);
        return params.zk.getChildren(params.path, watch, stat);
    }

    /**
     * @param path the given path, if remote action then starts with zkid of the relevant remote zookeeper. I.e., ZK1~/znode/...
     * @see org.apache.zookeeper.ZooKeeper#getChildren(String, Watcher, Stat, Children2Callback, Object)
     */
    public void getChildren(final String path, Watcher watcher,
            final Children2Callback cb, Object ctx){
        OpSharedParams params = addSyncIfNeededBeforeRead(path);
        outstanding.increase();
        params.zk.getChildren(params.path, watcher, new Children2Callback() {
            @Override
            public void processResult(int rc, String path, Object ctx, List children, Stat stat) {
                outstanding.decrease();
                cb.processResult(rc, path, ctx, children, stat);
            }
        }, ctx);
    }

    /**
     * @param path the given path, if remote action then starts with zkid of the relevant remote zookeeper. I.e., ZK1~/znode/...
     * @see org.apache.zookeeper.ZooKeeper#getChildren(String, boolean, Stat, Children2Callback, Object)
     */
    public void getChildren(String path, boolean watch, final Children2Callback cb,
            Object ctx){
        OpSharedParams params = addSyncIfNeededBeforeRead(path);
        outstanding.increase();
        params.zk.getChildren(params.path, watch, new Children2Callback() {
            
            @Override
            public void processResult(int rc, String path, Object ctx, List children, Stat stat) {
                outstanding.decrease();
                cb.processResult(rc, path, ctx, children, stat);
            }
        }, ctx);
    }

    /**
     * @param path the given path, if remote action then starts with zkid of the relevant remote zookeeper. I.e., ZK1~/znode/...
     * @see org.apache.zookeeper.ZooKeeper#removeWatches(String, Watcher, WatcherType, boolean)
     */
    public void removeWatches(String path, Watcher watcher,
            WatcherType watcherType, boolean local)
            throws InterruptedException, KeeperException {
        OpSharedParams params = setLastZKIDBeforeUpdate(path);
        params.zk.removeWatches(params.path, watcher, watcherType, local);
    }

    /**
     * @param path the given path, if remote action then starts with zkid of the relevant remote zookeeper. I.e., ZK1~/znode/...
     * @see org.apache.zookeeper.ZooKeeper#removeWatches(String, Watcher, WatcherType, boolean, VoidCallback, Object)
     */
    public void removeWatches(String path, Watcher watcher,
            WatcherType watcherType, boolean local, final VoidCallback cb, Object ctx) {
        OpSharedParams params = setLastZKIDBeforeUpdate(path);
        outstanding.increase();
        params.zk.removeWatches(params.path, watcher, watcherType, local, new VoidCallback() {
            
            @Override
            public void processResult(int rc, String path, Object ctx) {
                outstanding.decrease();
                cb.processResult(rc, path, ctx);
            }
        }, ctx);
    }

    /**
     * @param path the given path, if remote action then starts with zkid of the relevant remote zookeeper. I.e., ZK1~/znode/...
     * @see org.apache.zookeeper.ZooKeeper#removeWatches(String, Watcher, WatcherType, boolean)
     */
    public void removeAllWatches(String path, WatcherType watcherType,
            boolean local) throws InterruptedException, KeeperException {
        OpSharedParams params = setLastZKIDBeforeUpdate(path);
        params.zk.removeAllWatches(params.path, watcherType, local);
    }

    /**
     * @param path the given path, if remote action then starts with zkid of the relevant remote zookeeper. I.e., ZK1~/znode/...
     * @see org.apache.zookeeper.ZooKeeper#removeWatches(String, WatcherType, boolean, VoidCallback, Object)
     */
    public void removeAllWatches(String path, WatcherType watcherType,
            boolean local, final VoidCallback cb, Object ctx) {
        OpSharedParams params = setLastZKIDBeforeUpdate(path);
        outstanding.increase();
        params.zk.removeAllWatches(params.path, watcherType, local, new VoidCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx) {
                outstanding.decrease();
                cb.processResult(rc, path, ctx);
            }
        }, ctx);
    }

    /**
     * @param zkid of the ZK on which getState is called, empty for local zk
     * @see org.apache.zookeeper.ZooKeeper#getState()
     */
    public String getStatesString() {
        StringBuilder states = new StringBuilder();
        states.append("LocalZK-");
        states.append(super.getState());
        for (String zk : remoteZKIDToConnection.keySet()){
            if (zk.equals(ZooNet.LOCAL_ZKID)){
                continue;
            }
            states.append("|");
            states.append(zk);
            states.append("-");
            states.append(remoteZKIDToConnection.get(zk).getState());
        }
        return states.toString();
    }

    /**
     * @param zkid of the ZK on which toString is called, empty for local zk
     * @see org.apache.zookeeper.ZooKeeper#toString()
     */
    public String toString(String zkid) {
        OpSharedParams params = getZKByZKID(zkid);
        return params.zk.toString();
    }

}
