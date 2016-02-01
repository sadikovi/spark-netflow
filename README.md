# spark-netflow
A library for reading NetFlow files from [Spark SQL](http://spark.apache.org/docs/latest/sql-programming-guide.html).

[![Build Status](https://travis-ci.org/sadikovi/spark-netflow.svg?branch=master)](https://travis-ci.org/sadikovi/spark-netflow)
[![codecov.io](https://codecov.io/github/sadikovi/spark-netflow/coverage.svg?branch=master)](https://codecov.io/github/sadikovi/spark-netflow?branch=master)

## Requirements
| Spark version | spark-netflow version |
|---------------|-----------------------|
| 1.4+ | [0.0.2](http://spark-packages.org/package/sadikovi/spark-netflow) |

## Linking
The spark-netflow library can be added to Spark by using the `--packages` command line option. For
example, run this to include it when starting the spark shell:
```shell
 $SPARK_HOME/bin/spark-shell --packages sadikovi:spark-netflow:0.0.2-s_2.10
```

## Features
- Column pruning

### Options
Currently supported options:

| Name | Example | Description |
|------|:-------:|-------------|
| `version` | _5_ | version to use when parsing NetFlow files
| `buffer` | _1024, 32Kb, 3Mb, etc_ | buffer size for NetFlow compressed stream (default: 3Mb)
| `stringify` | _true, false_ | convert certain fields (e.g. IP) into human-readable format (default: false)

## Example

### Scala API
```scala
val sqlContext = new SQLContext(sc)

val df = sqlContext.read.format("com.github.sadikovi.spark.netflow").
  option("version", "5").load("file:/...").
  select("srcip", "dstip", "packets")

// You can also specify buffer size when reading compressed NetFlow files
val df = sqlContext.read.format("com.github.sadikovi.spark.netflow").
  option("version", "5").option("buffer", "50Mb").load("file:/...")
```

Alternatively you can use shortcuts for NetFlow v5 files
```scala
// for Spark 1.5+
val df = sqlContext.read.format("netflow").option("version", "5").load("file:/...")
```

```scala
import com.github.sadikovi.spark.netflow._

// this will read version 5 with default buffer size
val df = sqlContext.read.netflow("file:/...")
```

### Python API
```python
df = sqlContext.read.format("com.github.sadikovi.spark.netflow").option("version", "5").
  load("file:/...").select("srcip", "srcport")

res = df.where("srcip > 10")
```

## Building From Source
This library is built using `sbt`, to build a JAR file simply run `sbt package` from project root.
