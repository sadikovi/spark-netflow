# spark-netflow
A library for reading NetFlow files from [Spark SQL](http://spark.apache.org/docs/latest/sql-programming-guide.html).

## Linking
The spark-netflow library can be added to Spark by using the `--packages` command line option. For
example, run this to include it when starting the spark shell:
```shell
 $SPARK_HOME/bin/spark-shell --packages sadikovi:spark-netflow:0.0.1-s_2.10
```

## Features
- Column pruning

### Options
Currently supported options:

| Name | Example | Description |
|------|:-------:|-------------|
| `version` | **5** | version to use when parsing NetFlow files

## Example

### Scala API
```scala
val sqlContext = new SQLContext(sc)

val df = sqlContext.read.format("com.github.sadikovi.spark.netflow").
  option("version", "5").load("file:/...").
  select("srcip", "dstip", "packets")
```

## Building From Source
This library is built using `sbt`, to build a JAR file simply run `sbt package` from project root.
