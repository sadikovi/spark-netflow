## NetFlow version 7

This is a list of columns that are supported in **spark-netflow**. Note that some fields have two
different types associated with them, this means that those fields have `String` representation of
their values (handled with `stringify` option).

| # | Column name | Column type (Spark SQL) | Description |
|---|-------------|-------------------------|-------------|
| 1 | `unix_secs` | `LongType` | Current seconds since 0000 UTC 1970 |
| 2 | `unix_nsecs` | `LongType` | Residual nanoseconds since 0000 UTC 1970 |
| 3 | `sysuptime` | `LongType` | Current time in milliseconds since router booted |
| 4 | `exaddr` | `LongType` / `StringType` | Exporter IP address |
| 5 | `srcip` | `LongType` / `StringType` | Source IP address |
| 6 | `dstip` | `LongType` / `StringType` | Destination IP address |
| 7 | `nexthop` | `LongType` / `StringType` | Next hop router's IP address |
| 8 | `input` | `IntegerType` | Input interface index (known as Sif) |
| 9 | `output` | `IntegerType` | Output interface index (known as Dif) |
| 10 | `packets` | `LongType` | Packets sent in duration |
| 11 | `octets` | `LongType` | Octets sent in duration |
| 12 | `first_flow` | `LongType` | System uptime at start of flow |
| 13 | `last_flow` | `LongType` | System uptime of last packet of flow |
| 14 | `srcport` | `IntegerType` | TCP/UDP source port number or equivalent |
| 15 | `dstport` | `IntegerType` | TCP/UDP destination port number or equivalent |
| 16 | `protocol` | `ShortType` / `StringType` | IP protocol, e.g. 6 = TCP, 17 = UDP, etc. |
| 17 | `tos` | `ShortType` | IP Type-of-Service |
| 18 | `tcp_flags` | `ShortType` | OR of TCP header bits |
| 19 | `flags` | `ShortType` | Reason flow discarded |
| 20 | `engine_type` | `ShortType` | Type of flow switching engine (RP, VIP, etc.) |
| 21 | `engine_id` | `ShortType` | Slot number of the flow switching engine |
| 22 | `src_mask` | `ShortType` | Mask length of source address |
| 23 | `dst_mask` | `ShortType` | Mask length of destination address |
| 24 | `src_as` | `IntegerType` | AS of source address |
| 25 | `dst_as` | `IntegerType` | AS of destination address |
| 26 | `router_sc` | `LongType` / `StringType` | ID of router shortcut by switch |
