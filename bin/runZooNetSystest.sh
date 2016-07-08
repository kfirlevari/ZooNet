#!/bin/bash

TEST_ZNODE="/ZooNetLoad"
BASE_OF_ZK="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )""/../"
CFGDir=""$BASE_OF_ZK"/cfg/zoonetLoad"
ZKJAR=""$BASE_OF_ZK"/build/contrib/fatjar/zookeeper-dev-fatjar.jar"
CONTAINERS_PER_ZK=4 # here we assume 2 ZKs

clean ()
{
	while read p; do
	  kill -9 $p
	done <"$BASE_OF_ZK"/logs/toKill.list
	rm "$BASE_OF_ZK"/logs/toKill.list
	rm -r /tmp/mdzk/*
}

runTests ()
{
	clean
	[ -d "$BASE_OF_ZK"/logs/znSysTest_logs ] || mkdir -p "$BASE_OF_ZK"/logs/znSysTest_logs
	for confFile in "$CFGDir"/*
	do
		confNameWithExt=${confFile##*/}
	
		# run MDZK
		###########
		if [ -d /tmp/mdzk ]; then 
 			rm -r /tmp/mdzk
		fi
		java -jar $ZKJAR server "$BASE_OF_ZK"/conf/mdzk.cfg  &> "$BASE_OF_ZK"/logs/mdzk.log &
		echo $! >> "$BASE_OF_ZK"/logs/toKill.list
		sleep 5

		# run containers
		#################
		java -jar $ZKJAR ic ic_1 127.0.0.1:2181 "$TEST_ZNODE" &> "$BASE_OF_ZK"/logs/container1.log &
		echo $! >> "$BASE_OF_ZK"/logs/toKill.list
		java -jar $ZKJAR ic ic_2 127.0.0.1:2181 "$TEST_ZNODE" &> "$BASE_OF_ZK"/logs/container2.log &
		echo $! >> "$BASE_OF_ZK"/logs/toKill.list
		java -jar $ZKJAR ic ic_ZK1AppA1 127.0.0.1:2181 "$TEST_ZNODE" &> "$BASE_OF_ZK"/logs/containerZK1AppA1.log &
		echo $! >> "$BASE_OF_ZK"/logs/toKill.list
		java -jar $ZKJAR ic ic_ZK2AppB1 127.0.0.1:2181 "$TEST_ZNODE" &> "$BASE_OF_ZK"/logs/containerZK2AppB1.log &
		echo $! >> "$BASE_OF_ZK"/logs/toKill.list

		sleep 5

		# run Test
		###########
		java -jar $ZKJAR znSysTest "$CFGDir"/"$confNameWithExt" &
		sleep 120
				
		clean
	done
}

generateCfgs ()
{
	[ -d "$CFGDir" ] || mkdir -p "$CFGDir"
	rm "$CFGDir"/*
	outstandingLimit=100
	burstSize=25
	clients=3
	binsNumber=100
	for spacialLocalityRatio in "50" "90"
	do
	    for readRatio in "50" "99" 
	    do
		cfgFile="$CFGDir"/znSysTest_"$readRatio""-""$spacialLocalityRatio"_DC"$CONTAINERS_PER_ZK".cfg
		[ -a "$cfgFile" ] && rm "$cfgFile"
		echo "metaDataZK=127.0.0.1:2181" >> "$cfgFile"
		echo "testPrefix=$TEST_ZNODE" >> "$cfgFile"
		echo "printOnlyForGraph=true" >> "$cfgFile"
		echo "burstSize=$burstSize" >> "$cfgFile"
		echo "outstandingLimit=$outstandingLimit" >> "$cfgFile"
		echo "leaderServes=false" >> "$cfgFile"
		echo "binsNumber=$binsNumber" >> "$cfgFile"
		echo "testIP=127.0.0.1" >> "$cfgFile"

		#ZK1
		serversString="ZKInfo_Servers_ZK1=ZK1|"
		CONTAINERID=1
		while [  $CONTAINERID -lt $CONTAINERS_PER_ZK ]; do
		    serversString+="ic_1:1"$CONTAINERID":participant:127.0.0.1;"
		    let CONTAINERID=CONTAINERID+1 
		done
		serversString+="ic_2:14:observer:127.0.0.1"
		echo "$serversString" >> "$cfgFile"

		#ZK2
		serversString="ZKInfo_Servers_ZK2=ZK2|"
		CONTAINERID=1
		while [  $CONTAINERID -lt $CONTAINERS_PER_ZK ]; do
		    serversString+="ic_2:2"$CONTAINERID":participant:127.0.0.1;"
		    let CONTAINERID=CONTAINERID+1 
		done
		serversString+="ic_1:24:observer:127.0.0.1"
		echo "$serversString" >> "$cfgFile"

		#Connect App1 (a set of clients) to ZK1
		CID=1
		appString="AppInfo_A1=AppNameA_1|""$readRatio""-""$spacialLocalityRatio""|1024|""$clients""|ZK1-"
		while [  $CID -lt $CONTAINERS_PER_ZK ]; do
			appString+="ic_1:1""$CID"
			let CID=CID+1 
			if [ $CID -lt $CONTAINERS_PER_ZK ] 
			then
				appString+=","
			fi
		done
		appString+="|ic_ZK1AppA1|ZK2-ic_1:24"
		echo "$appString" >> "$cfgFile"

		#Connect App2 (a set of clients) to ZK2
		CID=1
		appString="AppInfo_B1=AppNameB_1|""$readRatio""-""$spacialLocalityRatio""|1024|""$clients""|ZK2-"
		while [  $CID -lt $CONTAINERS_PER_ZK ]; do
			appString+="ic_2:2""$CID"
			let CID=CID+1 
			if [ $CID -lt $CONTAINERS_PER_ZK ] 
			then
				appString+=","
			fi
		done
		appString+="|ic_ZK2AppB1|ZK1-ic_2:14"
		echo "$appString" >> "$cfgFile"
	    done 
	done
}

cd "$BASE_OF_ZK"

for iter in 1
do
        generateCfgs
        runTests
        mv "$BASE_OF_ZK"/logs/znSysTest_logs "$BASE_OF_ZK"/logs/znSysTest_logs_i"$iter"
	mv "$BASE_OF_ZK"/*.log "$BASE_OF_ZK"/logs/znSysTest_logs_i"$iter"
done
