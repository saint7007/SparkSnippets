import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

//scala> val schemaString= stockRDD.first()
//schema: String = SYMBOL,SERIES,OPEN,HIGH,LOW,CLOSE,LAST,PREVCLOSE,TOTTRDQTY,TOTTRDVAL,TIMESTAMP,

//scala> val schemaString= stockRDD.first().dropRight(1)
//schema: String = SYMBOL,SERIES,OPEN,HIGH,LOW,CLOSE,LAST,PREVCLOSE,TOTTRDQTY,TOTTRDVAL,TIMESTAMP

val dfreader = spark.read

val schema = StructType(Array(StructField("SYMBOL", StringType, true), StructField("SERIES", StringType, true), StructField("OPEN", DoubleType, true), StructField("HIGH", DoubleType, true), StructField("LOW", DoubleType, true), StructField("CLOSE", DoubleType, true), StructField("LAST", DoubleType, true),  StructField("PREVCLOSE", DoubleType, true), StructField("TOTTRDQTY", IntegerType, true), StructField("TOTTRDVAL", DoubleType, true),  StructField("TIMESTAMP", StringType, true)))

val df = dfreader.option("header","true").schema(schema).csv("all.csv")


val dfRollSymbol = df.withColumn("MONTH",substring(col("TIMESTAMP"),4,3)).where("SYMBOL in ('LT','TCS','INFY') and SYMBOL != ''").rollup("SYMBOL").agg(Map("CLOSE" -> "avg")).where("SYMBOL is not null").limit(100);

val dfRollMonthly = df.withColumn("MONTH",substring(col("TIMESTAMP"),4,3)).where("SYMBOL in ('LT','TCS','INFY') and SYMBOL != ''").rollup("SYMBOL","MONTH").agg(Map("CLOSE" -> "avg")).where("SYMBOL is not null").limit(100);

dfRollSymbol.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("a1-p4-Symbol_sushant.out")

dfRollMonthly.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("a1-p4-Monthly_sushant.out")

