import org.apache.spark.sql.SQLContext
val sqlContext = new SQLContext(sc)
val csvFile = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("all.csv")
csvFile.registerTempTable("stock")
val select100=sqlContext.sql("select *  from stock LIMIT 100")
select100
select100.write.csv("A1-P1-Sushant.out");
