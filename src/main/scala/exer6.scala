import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.concat_ws
import org.apache.spark.sql.types.StringType

object exer6 extends App {
  /*
  * 1. Leer el contenido de customers-avro
  * 2. Mostrar solo las columnas id y nombre
  * 3. Unir por un espacio el nombre y el apellido
  * 4. Devolver el resultado por compresion bzip2
  * */
  implicit val sparkSession = Spark.createLocalSession
  implicit val custavro: DataFrame = MainRead.readAvro("src/main/resources/retail_db/customers-avro")
  //Comprobamos que esta bien
  custavro.show
  //Realizamos dicho filtro
  val sol6 = custavro.select(custavro.col("customer_id").cast(StringType),concat_ws(" ", custavro.col("customer_fname") , custavro.col("customer_lname"))
    .cast(StringType).as("name"))
  //Como se puede observar nos da bien
  sol6.show()

  //Lo guardamos
  sol6.write.option("codec", "org.apache.hadoop.io.compress.BZip2Codec").csv("dataset/q6/solution")

  //Otro método para guardarlo sin ser bzip2 como texto
  //sol6.rdd.map(_.toString().replace("[","").replace("]", "")).saveAsTextFile("dataset/q2/solution")
}
