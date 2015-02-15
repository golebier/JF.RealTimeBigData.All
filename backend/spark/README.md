## Spark with Java for the Currency Data

I used the Spark 1.2.0

for ide(idea is suported too):

  gradle eclipse

then:

one step clean/make/run/test
TODO clean ES and local data, make via gradle, rsync to test evns, run core flow, test
first step is to make it work in no real time mode, then moving it to realtime mode is very easy.
But testing in not realtime mode is very easy.

TODO prepare tests for realtime too --- 1PB Currencies by "time" through flume, then changed above and realtime test at ES.
                                        throw 60klps, 70klps, ..., 400klps, more? Performance test + result validation -- expected results with correctness and in expected time.

TODO realtime component corrected by batch one, on each 5 min(one batch computation time).

--- TODO test a full flow realtime and batch time with 1PB Currencies ... compilation of both tests. --- one step to fuul fun. test with read UI and validate results --- 

-------------------------------------------------------
 
The master or all, should be ran:
  $SPARK/sbin/start-all.sh 

#### tested on:
spark-1.2.0

$ hadoop version
Hadoop 2.5.2
Subversion https://git-wip-us.apache.org/repos/asf/hadoop.git -r cc72e9b000545b86b75a61f4835eb86d57bfafc0
Compiled by jenkins on 2014-11-14T23:45Z
Compiled with protoc 2.5.0
From source with checksum df7537a4faa4658983d397abf4514320
This command was run using /$DIR/hadoop-2.5.2/share/hadoop/common/hadoop-common-2.5.2.jar

$ java -version
java version "1.7.0_71"
Java(TM) SE Runtime Environment (build 1.7.0_71-b14)
Java HotSpot(TM) 64-Bit Server VM (build 24.71-b01, mixed mode)

$ gradle -v

------------------------------------------------------------
Gradle 2.2.1
------------------------------------------------------------

Build time:   2014-11-24 09:45:35 UTC
Build number: none
Revision:     6fcb59c06f43a4e6b1bcb401f7686a8601a1fb4a

Groovy:       2.3.6
Ant:          Apache Ant(TM) version 1.9.3 compiled on December 23 2013
JVM:          1.7.0_71 (Oracle Corporation 24.71-b01)
OS:           Linux 3.17.6-1-ARCH amd64

$ uname -a
Linux ????? 3.17.6-1-ARCH #1 SMP PREEMPT Sun Dec 7 23:43:32 UTC 2014 x86_64 GNU/Linux

