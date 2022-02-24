import org.apache.spark.sql._
object Spark  {
  def createLocalSession= {
    val sparkSession = SparkSession.builder().master("local[*]").appName("adrian.moreno")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    sparkSession.sparkContext.setLogLevel("ERROR")
    sparkSession
  }


}
