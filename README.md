# spark-netflow
A library for reading NetFlow files from [Spark SQL](http://spark.apache.org/docs/latest/sql-programming-guide.html).

[![Build Status](https://travis-ci.org/sadikovi/spark-netflow.svg?branch=master)](https://travis-ci.org/sadikovi/spark-netflow)
[![codecov](https://codecov.io/gh/sadikovi/spark-netflow/branch/master/graph/badge.svg)](https://codecov.io/gh/sadikovi/spark-netflow)

## Requirements
| Spark version | spark-netflow latest version |
|---------------|------------------------------|
| 1.4+ | [1.1.0](http://spark-packages.org/package/sadikovi/spark-netflow) |

## Linking
The spark-netflow library can be added to Spark by using the `--packages` command line option. For
example, run this to include it when starting the spark shell:
```shell
 $SPARK_HOME/bin/spark-shell --packages sadikovi:spark-netflow:1.1.0-s_2.10
```
Change to `sadikovi:spark-netflow:1.1.0-s_2.11` for Scala 2.11.x

## Features
- Column pruning
- Predicate pushdown to the NetFlow file
- Auto statistics on `unix_secs` (filter records and entire files based on predicate)
- Manual statistics on certain columns depending on version (when option provided)
- Fields conversion (IP addresses, protocol, etc.)
- NetFlow version 5 support ([list of columns](./docs/NETFLOW_V5.md))
- NetFlow version 7 support ([list of columns](./docs/NETFLOW_V7.md))
- Reading files from local file system and HDFS
- Different partition strategies

### Options
Currently supported options:

| Name | Since | Example | Description |
|------|:-----:|:-------:|-------------|
| `version` | `0.0.1` | _5, 7_ | version to use when parsing NetFlow files, your own version provider can be passed, by default will resolve from files provided
| `buffer` | `0.0.2` | _1024, 32Kb, 3Mb, etc_ | buffer size for NetFlow compressed stream (default: `1Mb`)
| `stringify` | `0.0.2` | _true, false_ | convert certain fields (e.g. IP, protocol) into human-readable format, though it is recommended to turn it off when performance matters (default: `true`)
| `predicate-pushdown` | `0.2.0` | _true, false_ | use predicate pushdown at NetFlow library level (default: `true`)
| `partitions` | `0.2.1` | _default, auto, 1, 2, 100, 1024, etc_ | partition mode to use, can be `default`, `auto`, or any number of partitions (default: `default`)
| `statistics` | `1.0.0` | _true, false, file:/.../, hdfs://.../_ | use manual statistics for certain columns, see details for more information (default: `false`)

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

### Details on statistics
**spark-netflow** supports collecting statistics for NetFlow files when option `statistics` is used.
Currently there are several values supported:
- `false` statistics are disabled, this is default value.
- `true` statistics are enabled, generated file is stored in the same directory as original file.
- `file:/.../` or `hdfs://.../` statistics are enabled, directory provided (can be local file
  system or HDFS) is considered to be a root of where statistics are stored. Package saves
  statistics files by reconstructing original file directory from the root provided, e.g. file
  location is `file:/tmp/netflow/ft-v5`, option value is `hdfs://.../dir`, then statistics file is
  stored as `hdfs://.../dir/tmp/netflow/.statistics-ft-v5`.

Columns that are used to collect statistics are version dependent. For version 5 and 7 `srcip`,
`dstip`, `srcport`, `dstport`, and `protocol` are used. Note that **statistics/filtering on time are
always enabled** since they are provided by original file.

Statistics are either written lazily or read, if available. Package automatically figures out which
files have and do not have statistics, and will perform writes or reads accordingly. Collecting
statistics is lazy, package will only collect them when conditions are met, such as no filters
specified when selecting data, columns that are selected contain all statistics columns, and
all data is scanned/requested. The easiest way to trigger that is running `count()` on DataFrame.
Using statistics does not require any special conditions apart from enabling option.

## Example

### Scala API
```scala
// You can provide only format, package will infer version from provided files, or you can enforce
// version of the files with `version` option.
val df = sqlContext.read.format("com.github.sadikovi.spark.netflow").load("...")

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
  target/scala-2.10/spark-netflow_2.10-1.1.0.jar \
  --iterations 5 \
  --files 'file:/Users/sadikovi/developer/spark-netflow/temp/ftn/0[1,2,3]/ft*' \
  --version 5
```

Latest benchmarks:
```
- Iterations: 5
- Files: file:/Users/sadikovi/developer/spark-netflow/temp/ftn/0[1,2,3]/ft*
- Version: 5
Running benchmark: NetFlow full scan
  Running case: Scan, stringify = F
  Running case: Scan, stringify = T                                             

Intel(R) Core(TM) i5-4258U CPU @ 2.40GHz
NetFlow full scan:                  Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
-------------------------------------------------------------------------------------------
Scan, stringify = F                       593 /  671       1686.2       59303.3       1.0X
Scan, stringify = T                      1264 / 1280        790.9      126431.2       0.5X

Running benchmark: NetFlow predicate scan
  Running case: Predicate pushdown = F, high
  Running case: Predicate pushdown = T, high                                    
  Running case: Predicate pushdown = F, low                                     
  Running case: Predicate pushdown = T, low                                     

Intel(R) Core(TM) i5-4258U CPU @ 2.40GHz
NetFlow predicate scan:             Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
-------------------------------------------------------------------------------------------
Predicate pushdown = F, high             1294 / 1305        773.0      129361.1       1.0X
Predicate pushdown = T, high             1306 / 1330        765.5      130628.6       1.0X
Predicate pushdown = F, low              1081 / 1127        924.8      108129.1       1.2X
Predicate pushdown = T, low               272 /  275       3673.6       27221.0       4.8X

Running benchmark: NetFlow aggregated report
  Running case: Aggregated report

Intel(R) Core(TM) i5-4258U CPU @ 2.40GHz
NetFlow aggregated report:          Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
-------------------------------------------------------------------------------------------
Aggregated report                        1551 / 1690        644.6      155143.4       1.0X
```

## Using `netflowlib` library separately
You can use `netflowlib` without using `spark-netflow` package. Here some basic concepts and
examples:
- `com.github.sadikovi.netflowlib.predicate.Columns.*` all available column types in the library,
check out `com.github.sadikovi.netflowlib.version.*` classes to see what columns are already defined
for a specific NetFlow format.
- `com.github.sadikovi.netflowlib.predicate.FilterApi` utility class to create predicates for
NetFlow file
- `com.github.sadikovi.netflowlib.statistics.StatisticsTypes` statistics that you can use to reduce
boundaries of filter or allow filter to be evaluated before scanning the file. For example, library
creates statistics on time, so time filter can be resolved upfront
- `com.github.sadikovi.netflowlib.NetFlowReader` main entry to work with NetFlow file, gives access
to file header and iterator of rows, allows to pass additional predicate and statistics
- `com.github.sadikovi.netflowlib.NetFlowHeader` header information can be accessed using this
class from `NetFlowReader.getHeader()`, see class for more information on flags available

Note that library has only one external dependency on `io.netty.buffer.ByteBuf` buffers, which
could be replaced with standard Java buffer functionality, but since it was built for being used as
part of a spark-package, this dependency comes with Spark.

Here is the general usage pattern:
```scala

import com.github.sadikovi.netflowlib.NetFlowReader
import com.github.sadikovi.netflowlib.version.NetFlowV5

// Create input stream by opening NetFlow file, e.g. `fs.open(hadoopFile)`
val stm: DataInputStream = ...
// Prepare reader based on input stream and buffer size, you can use
// overloaded alternative with default buffer size
val reader = NetFlowReader.prepareReader(stm, 10000)
// Check out header, optional
val header = reader.getHeader()
// Actual NetFlow version of the file
val actualVersion = header.getFlowVersion()
// Whether or not file is compressed
val isCompressed = header.isCompressed()

// This is list of fields that will be returned in iterator as values in
// array (same order)
val fields = Array(
  NetFlowV5.FIELD_UNIX_SECS,
  NetFlowV5.FIELD_SRCADDR,
  NetFlowV5.FIELD_DSTADDR,
  NetFlowV5.FIELD_SRCPORT,
  NetFlowV5.FIELD_DSTPORT
)

// Build record buffer and iterator that you can use to get values.
// Note that you can also use set of filters, if you want to get
// particular records
val recordBuffer = reader.prepareRecordBuffer(fields)
val iter = recordBuffer.iterator()

while (iter.hasNext) {
  // print every row with values
  println(iter.next)
}
```

Here is an example of using predicate to keep certain records:
```scala
import com.github.sadikovi.netflowlib.predicate.FilterApi
val predicate = FilterApi.and(
  FilterApi.eq(NetFlowV5.FIELD_SRCPORT, 123),
  FilterApi.eq(NetFlowV5.FIELD_DSTPORT, 456)
)

...
val recordBuffer = reader.prepareRecordBuffer(fields, predicate)
```
