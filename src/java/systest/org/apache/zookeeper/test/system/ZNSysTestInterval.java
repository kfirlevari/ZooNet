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

public class ZNSysTestInterval {
    static final long INTERVAL = 6000;

    Long localReadRequests;
    Long localWriteRequests;
    Long remoteReadRequests;
    Long remoteWriteRequests;
    NumericHistogram localReadLatency;
    NumericHistogram localWriteLatency;
    NumericHistogram remoteReadLatency;
    NumericHistogram remoteWriteLatency;
    
    
    NumericHistogram latency;
    
    public ZNSysTestInterval(Long localRead, Long localWrite, Long remoteRead, Long remoteWrite,
    		NumericHistogram localReadLatency, NumericHistogram localWriteLatency, NumericHistogram remoteReadLatency, NumericHistogram remoteWriteLatency){
       this.localReadRequests = (localRead == null) ? 0 : localRead;
       this.localWriteRequests = (localWrite == null) ? 0 : localWrite;
       this.remoteReadRequests = (remoteRead == null) ? 0 : remoteRead;
       this.remoteWriteRequests = (remoteWrite == null) ? 0 : remoteWrite;
       this.localReadLatency= localReadLatency;
       this.localWriteLatency= localWriteLatency;
       this.remoteReadLatency = remoteReadLatency;
       this.remoteWriteLatency= remoteWriteLatency;
    }
    
    public String toString(){
		return 
		"localReadRequests:"+ localReadRequests +
	    " localWriteRequests:"+localWriteRequests +
	    " remoteReadRequests:"+ remoteReadRequests +
	    " remoteWriteRequests:"+ remoteWriteRequests + 
	    " localReadLatency:"+ localReadLatency+
	    " localWriteLatency:"+localWriteLatency+
	    " remoteReadLatency:"+ remoteReadLatency+
	    " remoteWriteLatency:"+ remoteWriteLatency;
    }
}