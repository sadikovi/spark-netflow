# spark-netflow
A library for reading NetFlow files from [Spark SQL](http://spark.apache.org/docs/latest/sql-programming-guide.html).

[![Build Status](https://travis-ci.org/sadikovi/spark-netflow.svg?branch=master)](https://travis-ci.org/sadikovi/spark-netflow)
[![codecov.io](https://codecov.io/github/sadikovi/spark-netflow/coverage.svg?branch=master)](https://codecov.io/github/sadikovi/spark-netflow?branch=master)

## Requirements
| Spark version | spark-netflow latest version |
|---------------|------------------------------|
| 1.4+ | [0.2.2](http://spark-packages.org/package/sadikovi/spark-netflow) |

## Linking
The spark-netflow library can be added to Spark by using the `--packages` command line option. For
example, run this to include it when starting the spark shell:
```shell
 $SPARK_HOME/bin/spark-shell --packages sadikovi:spark-netflow:0.2.2-s_2.10
```

## Features
- Column pruning
- Predicate pushdown to the NetFlow file
- Statistics on `unix_secs` (read only files that pass predicate on that column)
- Fields conversion (IP addresses, protocol, etc.)
- NetFlow version 5 support ([list of columns](./docs/NETFLOW_V5.md))
- NetFlow version 7 support ([list of columns](./docs/NETFLOW_V7.md))
- Reading files from local file system and HDFS
- Different partition strategies

### Options
Currently supported options:

| Name | Since | Example | Description |
|------|:-----:|:-------:|-------------|
| `version` | `0.0.1` | _5, 7_ | version to use when parsing NetFlow files, your own version provider can be passed
| `buffer` | `0.0.2` | _1024, 32Kb, 3Mb, etc_ | buffer size for NetFlow compressed stream (default: `1Mb`)
| `stringify` | `0.0.2` | _true, false_ | convert certain fields (e.g. IP, protocol) into human-readable format, though it is recommended to turn it off when performance matters (default: `true`)
| `predicate-pushdown` | `0.2.0` | _true, false_ | use predicate pushdown at NetFlow library level (default: `true`)
| `partitions` | `0.2.1` | _default, auto, 1, 2, 100, 1024, etc_ | partition mode to use, can be `default`, `auto`, or any number of partitions (default: `default`)

### Details on partition mode
**spark-netflow** supports three different partition modes:
- `default` mode puts every file into its own partition (default behaviour since the first version
  of package).
- specific number of slices can be specified, e.g. `sqlContext.read.option("partitions", "210")`,
  this will use standard RDD functionality to split files into provided number of slices.
- `auto` will try to split provided files into partitions the best way possible following the rule
  that each partition should be as close as possible to the best partition size. Best partition size
  is chosen based on mean of the files' sizes (considering possible skewness of the dataset) and
  provided best size using `spark.sql.netflow.partition.size`, default is `144Mb`. Note that auto
  mode will not be triggered, if number of files is less than default minimum number of partitions
  `spark.sql.netflow.partition.num` with default `sparkContext.defaultParallelism * 2`. Still
  default values should be pretty good for most of the workloads, including compressed files.

Tweak settings for auto mode:
```scala
// Best partition size (note that will be compared to the truncated mean of files provided)
// Chosen to keep roughly 10,000,000 records in each partition, if possible
sqlContext.setConf("spark.sql.netflow.partition.size", "144Mb")
// Minimum number of partitions before considering auto mode, increases cluster utilization for
// small batches
sqlContext.setConf("spark.sql.netflow.partition.num", s"${sc.defaultParallelism * 2}")
```

## Example

### Scala API
```scala
val sqlContext = new SQLContext(sc)

// You can read files from local file system or HDFS
val df = sqlContext.read.format("com.github.sadikovi.spark.netflow").
  option("version", "5").load("file:/...").
  select("srcip", "dstip", "packets")

// You can also specify buffer size when reading compressed NetFlow files
val df = sqlContext.read.format("com.github.sadikovi.spark.netflow").
  option("version", "5").option("buffer", "50Mb").load("hdfs://sandbox:8020/tmp/...")
```

Alternatively you can use shortcuts for NetFlow files
```scala
import com.github.sadikovi.spark.netflow._

// this will read version 5 with default buffer size
val df = sqlContext.read.netflow5("hdfs:/...")

// this will read version 7 without fields conversion
val df = sqlContext.read.option("stringify", "false").netflow7("file:/...")
```

### Python API
```python
df = sqlContext.read.format("com.github.sadikovi.spark.netflow").option("version", "5").
  load("file:/...").select("srcip", "srcport")

res = df.where("srcip > 10")
```

### SQL API
```sql
CREATE TEMPORARY TABLE ips
USING com.github.sadikovi.spark.netflow
OPTIONS (path "file:/...", version "5");

SELECT srcip, dstip, srcport, dstport FROM ips LIMIT 10;
```

## Building From Source
This library is built using `sbt`, to build a JAR file simply run `sbt package` from project root.

## Testing
Run `sbt test` from project root.

## Running benchmark
Run `sbt package` to package project, next run `spark-submit` with following options:
```shell
$ spark-submit --class com.github.sadikovi.spark.benchmark.NetFlowReadBenchmark \
  target/scala-2.10/spark-netflow_2.10-0.2.2-SNAPSHOT.jar \
  --iterations 3 \
  --files 'file:/Users/sadikovi/developer/spark-netflow/temp/ftn/*/ft*' \
  --version 5
```

Output will be similar to this:
```
- Iterations: 3
- Files: file:/Users/sadikovi/developer/spark-netflow/temp/ftn/*/ft*
- Version: 5
Running benchmark: NetFlow full scan
  Running case: Scan w/o stringify, buffer: 10Kb
  Running case: Scan w/o stringify, buffer: 50Mb                                
  Running case: Scan w/ stringify, buffer: 10Kb                                 
  Running case: Scan w/ stringify, buffer: 50Mb                                 

Intel(R) Core(TM) i5-4258U CPU @ 2.40GHz
NetFlow full scan:                  Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
-------------------------------------------------------------------------------------------
Scan w/o stringify, buffer: 10Kb         2033 / 2227          0.0      203281.6       1.0X
Scan w/o stringify, buffer: 50Mb         2069 / 2103          0.0      206884.9       1.0X
Scan w/ stringify, buffer: 10Kb          2553 / 2602          0.0      255300.5       0.8X
Scan w/ stringify, buffer: 50Mb          2836 / 2953          0.0      283586.6       0.7X

Running benchmark: NetFlow predicate scan
  Running case: Filter scan w/ predicate pushdown
  Running case: Filter scan w/o predicate pushdown                              

Intel(R) Core(TM) i5-4258U CPU @ 2.40GHz
NetFlow predicate scan:             Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
-------------------------------------------------------------------------------------------
Filter scan w/ predicate pushdown        1174 / 1231          0.0      117373.4       1.0X
Filter scan w/o predicate pushdown       1040 / 1135          0.0      104024.8       1.1X

Running benchmark: NetFlow aggregated report
  Running case: Aggregated report

Intel(R) Core(TM) i5-4258U CPU @ 2.40GHz
NetFlow aggregated report:          Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
-------------------------------------------------------------------------------------------
Aggregated report                        1448 / 1492          0.0      144783.1       1.0X
```
