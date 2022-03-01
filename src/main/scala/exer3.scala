import org.apache.spark.sql._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
object exer3 extends App {
  /*
  * 1.Leer el contenido de customer-tab-delimited
  * 2. Dar solo aquellos cuyo nombre empiece por A
  * 3. Hacer un recuento de cuantos hay por estado
  * 4. Quedarnos con aquellos cuyo recuento sea mayor que 50
  * 5. Solo mostrar columnas state y count
  * 6. La salida debe ser con formato parquet y con compresion gzip
  * */

  implicit val sparkSession = Spark.createLocalSession

//Me creo un schema para incluirlo en los datos
  val myschema= StructType(Array(
    StructField("id", IntegerType,true),
  StructField("lname", StringType,true),
  StructField("fname", StringType,true),
  StructField("email", StringType,true),
  StructField("password", StringType,true),
  StructField("street", StringType,true),
  StructField("city", StringType,true),
  StructField("state", StringType,true),
  StructField("zipcode", StringType,true)))

//Ahora que ya tenemos el schema, podemos leer los datos asignandole dicha cabecera

  implicit val customertabDF: DataFrame = MainRead.readCsvSchemaSpace("src/main/resources/retail_db/customers-tab-delimited/part-m-00000",myschema)

  val conteo = customertabDF.filter(col("fname").startsWith("A")).groupBy("state").count().filter(col("count").gt(50))
  //Comprobamos que esta bien
  //conteo.show()

// Procedo a guardarlo como se me indica
  conteo.write.mode(SaveMode.Overwrite).option("codec", "org.apache.hadoop.io.compress.GzipCodec").parquet("dataset/q3/customer-replica.parquet")

}
