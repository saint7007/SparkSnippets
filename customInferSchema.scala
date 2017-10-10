import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

val stockRDD=spark.sparkContext.textFile("all.csv")
//scala> val schemaString= stockRDD.first()
//schema: String = SYMBOL,SERIES,OPEN,HIGH,LOW,CLOSE,LAST,PREVCLOSE,TOTTRDQTY,TOTTRDVAL,TIMESTAMP,

//scala> val schemaString= stockRDD.first().dropRight(1)
//schema: String = SYMBOL,SERIES,OPEN,HIGH,LOW,CLOSE,LAST,PREVCLOSE,TOTTRDQTY,TOTTRDVAL,TIMESTAMP

val schemaString= stockRDD.first().dropRight(1)// the last comma creating problem.
val fields = schemaString.split(",").map(fieldName => Array(StructField("SYMBOL", StringType, true), StructField("SERIES", StringType, true), StructField("OPEN", DoubleType, true), StructField("HIGH", DoubleType, true), StructField("LOW", DoubleType, true), StructField("CLOSE", DoubleType, true), StructField("LAST", DoubleType, true),  StructField("PREVCLOSE", DoubleType, true), StructField("TOTTRDQTY", IntegerType, true), StructField("TOTTRDVAL", DoubleType, true),  StructField("TIMESTAMP", StringType, true)))
val schema = StructType(fields)

val rowRDD = stockRDD.map(_.split(",",-1)).map(attributes=>Row(attributes(0),attributes(1),attributes(2),attributes(3),attributes(4),attributes(5),attributes(6),attributes(7),attributes(8),attributes(9),attributes(10).trim))

val stockDF = spark.createDataFrame(rowRDD, schema)
stockDF.createOrReplaceTempView("Stocks")
val results100 = spark.sql("SELECT * from Stocks LIMIT 100")

results100.collect.foreach(println) // printing lines to check data.
results100.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("a1-p2-sushant.out")





