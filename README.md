ZooNet - Modular Composition of ZooKeepers
============
 ZooNet is a prototype of our modular composition concept over ZooKeeper. ZooNet allows users to compose multiple ZooKeepers 
 ensembles in a consistent fashion, facilitating applications that execute in multiple regions. In ZooNet, clients that 
 access only local data suffer no performance penalty compared to working with a standard single ZooKeeper. Clients that use 
 remote and local ZooKeepers show up to 7x performance improvement compared to consistent solutions available today.

ZooNet appeared in [Usenix ATC'16](https://www.usenix.org/conference/atc16/technical-sessions/presentation/lev-ari), the paper can be found [here](http://kfirlevari.com/publication/download/modular-composition-of-coordination-services).

Please see the following class for the main logic: [ZooNet.java](https://github.com/kfirlevari/ZooNet/blob/trunk/src/java/main/org/apache/zookeeper/ZooNet.java) .

---------------

In order to run ZooNet's systest, run the following commands:
* "ant jar compile-test" (from ZK base dir).
* "ant jar" (from "src/contrib/fatjar").
* "./bin/runZooNetSystest.sh" (from ZK base dir). This script is found [here](https://github.com/kfirlevari/ZooNet/blob/trunk/bin/runZooNetSystest.sh).

After initialization, you'll see the following printouts once every 6 sec (of throughput and latency percentiles, per every application):

[app name]	

[# of local read requests]	[# of local update requests]	[# of remote read requests]	[# of remote update requests]	

Meadian:	[local reads, in ms]	[local updates, in ms]	[remote reads, in ms]	[remote updates, in ms]

0.95:	[local reads, in ms]	[local updates, in ms]	[remote reads, in ms]	[remote updates, in ms]

0.99:	[local reads, in ms]	[local updates, in ms]	[remote reads, in ms]	[remote updates, in ms]

---------------

For the latest information about Apache ZooKeeper, please visit:

   http://zookeeper.apache.org/
