import org.apache.spark.sql.SQLContext

val sqlContext = new SQLContext(sc)

val df = sqlContext.read.format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat").option("header","true").option("delimiter",",").option("inferSchema", "true").option("dateFormat", "dd-mm-yyyy").load("all.csv")


val monthlyDf=df.where("TIMESTAMP like '%JUN-2013'");

monthlyDf.show(10)

monthlyDf.registerTempTable("dfTable");

spark.conf.set("spark.sql.crossJoin.enabled", true);


val outPutDf = spark.sql("select a.TIMESTAMP as DATE1,a.SYMBOL as STOCK1,a.CLOSE as CLOSE1,a.TOTTRDQTY as TRADEQTY1,b.TIMESTAMP as DATE2,b.SYMBOL as STOCK2,b.CLOSE as CLOSE2,b.TOTTRDQTY as TRADEQTY2 from dfTable a ,dfTable b where a.SYMBOL!=b.SYMBOL and b.CLOSE between (a.CLOSE-(a.CLOSE*0.01)) and (a.CLOSE+(a.CLOSE*0.01))");

outPutDf.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("a1-p3-sushant.out")



