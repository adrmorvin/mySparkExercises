import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, desc, hour, regexp_extract, split, to_date}
import org.apache.spark.sql.types.IntegerType

object SparkNASA extends App{
  implicit val sparkSession = Spark.createLocalSession
  val weblogs: DataFrame = sparkSession.read.option("header",true).option("sep", " ").option("inferSchema","true").csv("src/main/resources/spark2/nasa_aug95.csv")
  //weblogs.printSchema()
  //¿Cuáles son los distintos protocolos web utilizados? Agrúpalos.

  val protocoldist = weblogs.withColumn("new_row",split(col("request"), " "))
    .select(col("new_row").getItem(2).as("protocolos")).filter(col("protocolos").like("%HT%/%")).distinct()
  //protocoldist.show

  //¿Cuáles son los códigos de estado más comunes en la web? Agrúpalos y ordénalos
  //para ver cuál es el más común.
  val communstatus = weblogs.groupBy("status").count().orderBy(col("count").desc)
  //communstatus.show

  //¿Y los métodos de petición (verbos) más utilizados?
  val peticiones = weblogs.withColumn("new_row",split(col("request"), " "))
    .select(col("new_row").getItem(0).as("peticion")).groupBy("peticion").count().orderBy(col("count").desc)
  //peticiones.show

  //¿Qué recurso tuvo la mayor transferencia de bytes de la página web?
  val transf = weblogs.filter(col("response_size").isNotNull)
    .select(col("requesting_host"), col("response_size")).orderBy(col("response_size").desc).limit(1)
  //transf.show

  //Además, queremos saber que recurso de nuestra web es el que más tráfico recibe. Es
  //decir, el recurso con más registros en nuestro log
  val traficrequest = weblogs.groupBy("request").count().orderBy(col("count").desc).limit(1)
  //trafichost.show

  //¿Qué días la web recibió más tráfico?
  val traficday = weblogs.groupBy(to_date(col("datetime"))).count().orderBy(col("count").desc).limit(1)
  //traficday.show
  //¿Cuáles son los hosts son los más frecuentes?
  val hotsfrecuentes =weblogs.groupBy("requesting_host").count().orderBy(col("count").desc).limit(1)
  //hotsfrecuentes.show
  //¿A qué horas se produce el mayor número de tráfico en la web?
  val hourtraffic = weblogs.groupBy(hour(col("datetime"))).count().orderBy(col("count").desc)
  //hourtraffic.show()
  //¿Cuál es el número de errores 404 que ha habido cada día?
  val error404 = weblogs.filter(col("status").equalTo(404)).groupBy(to_date(col("datetime"))).count().orderBy(col("count").desc)
  //error404.show()
}
