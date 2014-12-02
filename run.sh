REMOTE_OUT=hdfs:///user/lgriffiths/communities
LOCAL_HOME=/home/lgriffiths/data/communities/
LOCAL_OUT=$LOCAL_HOME/hadoopOutput
SHARED_DATA=/home/lgriffiths/data/sharedData/


INFILE="hdfs:///stream_archives/2014/ovi.*.2014-05-27_17.log.gz"  #one hour
#INFILE="hdfs:///stream_archives/2014/keyword_clicks.*.2014-0[456]-*.log.gz*"
#INFILE="hdfs:///stream_archives/2014/keyword_clicks.*.2014-05-27_*"
#INFILE="hdfs:///stream_archives/2014/ovi.*.2014-05-27_*"         #one day
#INFILE="hdfs:///stream_archives/2014/ovi.*.2014-0[456]-*.log.gz"  #two months
#INFILE="hdfs:///stream_archives/2014/ovi.*.2014-05-*.log.gz" 

function panic
{
    echo $1
    exit 1
}

rm -f app.tar
tar -cf app.tar app/
userWhitelist=uniqueUsers.txt
bitlyToolsList=bitlyToolsID.txt

#type='raw'
type='rollupSmall'
LOCAL_OUT=$LOCAL_HOME/hadoopOutput_$type
hadoop fs -rm -r -skipTrash $REMOTE_OUT
set -x
python2.7  pull_mr.py \
    --python-archive='app.tar#tar' \
    -o $REMOTE_OUT \
    --no-output \
    --converter=$type   \
     --jobconf 'runners.hadoop.python_bin=/bitly/local/bin/python2.7' \
    --jobconf mapred.job.name=pull_data \
    --jobconf mapred.reduce.tasks=120 \
    --jobconf mapred.output.compress=true \
    --jobconf mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec \
    -r hadoop \
    --tlds='/bitly/data/effective_tld_names.dat' \
    $INFILE || panic "Hadoop job failed"


rm -rf $LOCAL_OUT

hadoop fs -get $REMOTE_OUT $LOCAL_OUT
hadoop fs -rm -r -skipTrash $REMOTE_OUT

zcat $LOCAL_OUT/part-*.gz | awk '{gsub("\"", ""); print $2" "$1}' | sort -rn > NYTimesClicks.log

set +x