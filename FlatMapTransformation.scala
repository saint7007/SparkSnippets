
import org.apache.spark.sql.SparkSession
import org.miz.helper.Helper._

object FlatMapTransformation {
  def main(args: Array[String]): Unit = {

    suppressLogs(List("org", "akka"))

    val spark = SparkSession.builder
      .master("local")
      .appName("Spark Graph Frames")
      .getOrCreate()

    val lines = spark.sparkContext.textFile("/home/amsys/sample.txt")

    val excludeHeader=lines.mapPartitionsWithIndex(
      (index, it) => if (index == 0) it.drop(1) else it,
      preservesPartitioning = true
    )

   val result= excludeHeader.flatMap(x => {

     val splitted=x.split("\\|")

     val id=splitted(0)
     val value1=splitted(1).split(";")

     val value2= value1(0).split(":")
     val value3= value1(1).split(":")

     List((id,value2(0)),(id,value2(1)),(id,value3(0)),(id,value3(1)))

   })
    result.collect();
    result.foreach(println)
  }

}
