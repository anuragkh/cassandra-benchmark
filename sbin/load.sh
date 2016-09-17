hostname=${1:-localhost}
dataset=${2:-tpch}

sbin="`dirname "$0"`"
sbin="`cd "$sbin"; pwd`"

if [ "$dataset" = "tpch" ]; then
  num_attr=16
elif [ "$dataset" = "conviva" ]; then
  num_attr=104
else
  echo "Invalid dataset"
  exit -1
fi

$sbin/../bin/cassandra-bench -h $hostname -i ~/$dataset -a $num_attr -d $dataset -l
