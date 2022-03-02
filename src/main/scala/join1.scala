import exer4.soccersol
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions.{col, desc}

object join1 extends App {
  implicit val sparkSession = Spark.createLocalSession
  val customDF: DataFrame = MainRead.readCsvHeader("src/main/resources/retail_db/customers")
  val ordersDF: DataFrame = sparkSession.read.csv("src/main/resources/retail_db/orders")

  val joinSol = ordersDF.groupBy(col("_c2")).count().where(col("count").gt(5))
    .join(customDF, ordersDF.col("_c2") === customDF.col("customer_id") , "inner" )
    .select("customer_fname","customer_lname", "count" )
    .filter(col("customer_fname").startsWith("M"))
    .orderBy(desc("count"))
  joinSol.show()

  joinSol.write.mode(SaveMode.Overwrite).option("codec", "org.apache.hadoop.io.compress.GzipCodec").option("sep", "|").csv("dataset/q7/solution")
}
