import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, desc}

object exer2 extends App {
 /*
 * 1. Leer el contenido de categories => como la lectura es similar que
 * otros ejercicios lo hice con categories-header
 * 2. Mostrar unicamente c_id y c_name
 * 3. ordenar por categoria de manera descendente
 * 4. Se desea que el output esté separado por ":" y en un único archivo
 * */
  implicit val sparkSession = Spark.createLocalSession
  implicit val categoriesDF: DataFrame = MainRead.readCsvHeader("src/main/resources/retail_db/categories-header")
  //Ya tenemos el contenido en el valor categoriesDF
  val catgr= categoriesDF.select(col("category_id").cast("int"),col("category_name")).orderBy(desc("category_id"))
  //Tras los filtros obtenemos el resultado
  //catgr.show
  // Lo guardamos en el directorio hdfs y en este proyecto
  catgr.coalesce(1).write.option("header","True").option("delimiter", ":").csv("hdfs://L2112050:4040/dataset/q2.csv")
  catgr.coalesce(1).write.mode(SaveMode.Overwrite).csv("dataset/q2/solution")
}
