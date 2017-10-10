import org.apache.spark.sql.SQLContext
val sqlContext = new SQLContext(sc)
val csvFile = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("all.csv")

csvFile.cube("SYMBOL","TIMESTAMP").agg(Map("CLOSE"->"avg","OPEN"->"max")).where("TIMESTAMP is not null").show(10);
//outPut

//scala> csvFile.cube("SYMBOL","TIMESTAMP").agg(Map("CLOSE"->"avg","OPEN"->"max")).where("TIMESTAMP is not null").show(10);
//[Stage 15:=============================>                            (1 + 1) / 2]17/07/16 23:23:34 WARN TaskMemoryManager: leak 8.5 MB //////memory //from org.apache.spark.unsafe.map.BytesToBytesMap@6fec5c84
//17/07/16 23:23:34 WARN TaskMemoryManager: leak a page: org.apache.spark.unsafe.memory.MemoryBlock@32159626 in task 424
//17/07/16 23:23:34 WARN TaskMemoryManager: leak a page: org.apache.spark.unsafe.memory.MemoryBlock@1719d2dd in task 424
//17/07/16 23:23:34 WARN Executor: Managed memory leak detected; size = 8912896 bytes, TID = 424
//+----------+-----------+----------+---------+                                   
//|    SYMBOL|  TIMESTAMP|avg(CLOSE)|max(OPEN)|
//+----------+-----------+----------+---------+
//|    A2ZMES|01-APR-2011|     289.2|    281.8|
//|   GMDCLTD|01-APR-2011|    139.25|   135.25|
//|  MICROSEC|01-APR-2011|      42.6|    41.95|
//|NAGREEKEXP|01-APR-2011|      31.4|    26.25|
//|SHRIRAMEPC|01-APR-2011|     161.4|    158.4|
//| THINKSOFT|01-APR-2011|      61.8|     57.7|
//|ABIRLANUVO|01-APR-2013|    984.55|    982.0|
//|     CAIRN|01-APR-2013|    286.05|    274.4|
//|FEDDERLOYD|01-APR-2013|      33.2|    32.95|
//| GUJRATGAS|01-APR-2013|    242.55|    239.9|
//+----------+-----------+----------+---------+
//only showing top 10 rows


csvFile.withColumn("MONTH",substring(col("TIMESTAMP"),4,3)).where("SYMBOL in ('LT','TCS','INFY') and SYMBOL != ''").cube("SYMBOL","SERIES").agg(Map("CLOSE" -> "avg")).where("SYMBOL is not null").show(10);
//OutPut
//scala> csvFile.withColumn("MONTH",substring(col("TIMESTAMP"),4,3)).where("SYMBOL in ('LT','TCS','INFY') and SYMBOL != ''").cube//("SYMBOL","SERIES").agg(Map("CLOSE" -> "avg")).where("SYMBOL is not null").show(10);
//+------+------+------------------+                                              
//|SYMBOL|SERIES|        avg(CLOSE)|
//+------+------+------------------+
//|   TCS|    EQ|1678.2980194017796|
//|    LT|    EQ|1479.9713015359746|
//|  INFY|    EQ| 2709.348041136145|
//|  INFY|    BL|           2168.05|
//|   TCS|    BL|1519.8999999999999|
//|    LT|  null|1480.0537156704365|
//|   TCS|  null| 1677.914798387098|
//|  INFY|  null|2708.2897849462406|
//|    LT|    BL|            1582.0|
//+------+------+------------------+



csvFile.withColumn("MONTH",substring(col("TIMESTAMP"),4,3)).where("SYMBOL in ('LT','TCS','INFY') and SYMBOL != ''").rollup("SYMBOL","MONTH").agg(Map("CLOSE" -> "avg")).where("SYMBOL is not null").show(10);;

//OutPut

scala> csvFile.withColumn("MONTH",substring(col("TIMESTAMP"),4,3)).where("SYMBOL in ('LT','TCS','INFY') and SYMBOL != ''").rollup("SYMBOL","MONTH").agg(Map("CLOSE" -> "avg")).where("SYMBOL is not null").show(10);
+------+-----+------------------+                                               
|SYMBOL|MONTH|        avg(CLOSE)|
+------+-----+------------------+
|   TCS|  JUN|1735.4542452830192|
|  INFY|  JUL| 2413.516666666666|
|   TCS|  FEB|1691.0892857142858|
|   TCS|  DEC|1632.6018867924524|
|  INFY|  DEC| 2639.096428571429|
|  INFY|  JAN| 2787.270454545455|
|    LT|  MAR|1456.7956310679608|
|    LT|  FEB|1424.6357142857144|
|   TCS|  APR|1703.9186170212763|
|   TCS|  JAN|1682.6370370370366|
+------+-----+------------------+
only showing top 10 rows

