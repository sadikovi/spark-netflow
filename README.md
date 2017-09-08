# spark-netflow
A library for reading NetFlow files from [Spark SQL](http://spark.apache.org/docs/latest/sql-programming-guide.html).

[![Build Status](https://travis-ci.org/sadikovi/spark-netflow.svg?branch=master)](https://travis-ci.org/sadikovi/spark-netflow)
[![codecov](https://codecov.io/gh/sadikovi/spark-netflow/branch/master/graph/badge.svg)](https://codecov.io/gh/sadikovi/spark-netflow)

## Requirements
| Spark version | spark-netflow latest version |
|---------------|------------------------------|
| 1.4.x | [1.3.1](http://spark-packages.org/package/sadikovi/spark-netflow) |
| 1.5.x | [1.3.1](http://spark-packages.org/package/sadikovi/spark-netflow) |
| 1.6.x | [1.3.1](http://spark-packages.org/package/sadikovi/spark-netflow) |
| 2.0.x | [2.0.4](http://spark-packages.org/package/sadikovi/spark-netflow) |
| 2.1.x | [2.0.4](http://spark-packages.org/package/sadikovi/spark-netflow) |

> Documentation reflects changes in master branch, for documentation on a specific version, please
> select corresponding version tag or branch.

## Linking
The spark-netflow library can be added to Spark by using the `--packages` command line option. For
example, run this to include it when starting the spark shell:
```shell
 $SPARK_HOME/bin/spark-shell --packages com.github.sadikovi:spark-netflow_2.11:2.0.4
```
Change to `com.github.sadikovi:spark-netflow_2.10:2.0.4` for Scala 2.10.x

## Features
- Column pruning
- Predicate pushdown to the NetFlow file
- Auto statistics based on file header information
- Fields conversion (IP addresses, protocol, etc.)
- NetFlow version 5 support ([list of columns](./docs/NETFLOW_V5.md))
- NetFlow version 7 support ([list of columns](./docs/NETFLOW_V7.md))
- Reading files from local file system and HDFS

### Options
Currently supported options:

| Name | Example | Description |
|------|:-------:|-------------|
| `version` | _5, 7_ | version to use when parsing NetFlow files, can be your own version provider as class name. Optional, by default will resolve from provided files
| `buffer` | _1024, 32Kb, 3Mb, etc_ | buffer size for NetFlow compressed stream (default `1Mb`)
| `stringify` | _true, false_ | convert certain supported fields (e.g. IP, protocol) into human-readable format. If performance is essential consider disabling feature (default `true`)
| `predicate-pushdown` | _true, false_ | enable predicate pushdown at NetFlow library level (default `true`)

### Dealing with corrupt files
Package supports Spark option `spark.files.ignoreCorruptFiles`. When set to `true`, corrupt files
are ignored (corrupt header, wrong format) or partially read (corrupt data block in a middle of a
file). By default, option is set to `false`, meaning exception will be raised when such file is
encountered, this behaviour is similar to Spark.

## Example

### Scala API
```scala
// You can provide only format, package will infer version from provided files, or you can enforce
// version of the files with `version` option.
val df = spark.read.format("com.github.sadikovi.spark.netflow").load("...")

// You can read files from local file system or HDFS
val df = spark.read.format("com.github.sadikovi.spark.netflow").
  option("version", "5").load("file:/...").
  select("srcip", "dstip", "packets")

// You can also specify buffer size when reading compressed NetFlow files
val df = spark.read.format("com.github.sadikovi.spark.netflow").
  option("version", "5").option("buffer", "50Mb").load("hdfs://sandbox:8020/tmp/...")
```

Alternatively you can use shortcuts for NetFlow files
```scala
import com.github.sadikovi.spark.netflow._

// this will read version 5 with default buffer size
val df = spark.read.netflow5("hdfs:/...")

// this will read version 7 without fields conversion
val df = spark.read.option("stringify", "false").netflow7("file:/...")
```

### Python API
```python
df = spark.read.format("com.github.sadikovi.spark.netflow").option("version", "5").
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
To build jars for Scala 2.10.x and 2.11.x run `sbt +package`.

## Testing
Run `sbt test` from project root.

## Running benchmark
Run `sbt package` to package project, next run `spark-submit` with following options:
```shell
$ spark-submit --class com.github.sadikovi.spark.benchmark.NetFlowReadBenchmark \
  target/scala-2.11/spark-netflow_2.11-2.0.4.jar \
  --iterations 5 \
  --files 'file:/Users/sadikovi/developer/spark-netflow/temp/ftn/0[1,2,3]/ft*' \
  --version 5
```

Latest benchmarks:
```
- Iterations: 5
- Files: file:/Users/sadikovi/developer/spark-netflow/temp/ftn/0[1,2,3]/ft*
- Version: 5

Java HotSpot(TM) 64-Bit Server VM 1.7.0_80-b15 on Mac OS X 10.12.4
Intel(R) Core(TM) i5-4258U CPU @ 2.40GHz
NetFlow full scan:                       Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
------------------------------------------------------------------------------------------------
Scan, stringify = F                            567 /  633          0.0       56726.7       1.0X
Scan, stringify = T                            968 / 1049          0.0       96824.6       0.6X

Java HotSpot(TM) 64-Bit Server VM 1.7.0_80-b15 on Mac OS X 10.12.4
Intel(R) Core(TM) i5-4258U CPU @ 2.40GHz
NetFlow predicate scan:                  Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
------------------------------------------------------------------------------------------------
Predicate pushdown = F, high                  1148 / 1200          0.0      114845.4       1.0X
Predicate pushdown = T, high                  1208 / 1257          0.0      120818.0       1.0X
Predicate pushdown = F, low                    706 /  732          0.0       70559.3       1.6X
Predicate pushdown = T, low                    226 /  243          0.0       22575.0       5.1X

Java HotSpot(TM) 64-Bit Server VM 1.7.0_80-b15 on Mac OS X 10.12.4
Intel(R) Core(TM) i5-4258U CPU @ 2.40GHz
NetFlow aggregated report:               Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
------------------------------------------------------------------------------------------------
Aggregated report                             2171 / 2270          0.0      217089.9       1.0X
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
