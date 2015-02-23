# JF.RealTimeBigData.All

Components plan:
 - Backend:  Flume --> Spark(Java|Scala) --> ElasticSearch
 - Backend first satege: realtime to prepare in realtime all results for elasticsearch
 - Backend second satege: batch, load a big portion of jsons from hdfs and correct results for elasticsearch
 - Frontend: Sinatra/AngularJs or Just a AngularJs, realtime currency with google maps and location

Should be used ;) :
 Approach taken
 Architecture
 Code structure and clarity
 Performance(60k-400k lps)
 Security
 Testing

# I do not have too much time for this, I hope I can improve all in the future.
# done for now:

- Backend: real time with a single node 8 cores and 4GB memory for executor.
  - 60 000 jsons per second.
  - simple example:
  
    cat POSTs > netcat --> SparkFlow --> Elasticsearch

# needed:

linux ;) I used ArchLinux
gradle: 2.2.1 or above
spark: 1.2.1
elasticsearch: 1.4.4
netcat: 1.105 (openbsd-netcat)

# how to run:

--- clone this procject to $PROJECT ;)

--- first terminal:
cd $PROJECT/scripts
bash MakeNHsTestFileWithMkJsonsPerSec.Bsh # prepares 1h test file with 60000 jsons per second, It can take a bit, then U can use: Prepare60000jsonsForTests.Bsh
bash ASimpleTestFlowPostToEs.Bsh # if U used Prepare60000jsonsForTests.Bsh then change in SendCFPosts.Bsh file name

--- second terminal:
$ELASTICSEARCH/bin/elasticsearch # wait a sec for ES initialization


--- third terminal:
$SPARK/sbin/start-all.sh
cd $PROJECT/backend/spark/all-currencies-batch
bash ../../../scripts/SparkStreamingFlow.Bsh # change PUT_Ur_SPARK_MASTER_URL to Ur spark master URL with port

--- fourth terminal:
while true; do curl -XGET 'localhost:9200/currency_streaming_spark_dev/_search/?pretty=1'; done # U'll see all received results in realtime
bash ../../../scripts/streaming_and_batch_es_mapping_spark.Bsh # to clean indexes

# TODO
--- backend with batch computations and saving correction results to ES
--- UI

