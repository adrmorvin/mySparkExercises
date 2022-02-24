import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object exer4 extends App {
  /*
  * 1. Leemos el contenido de categories
  * 2. Nos quedamos con los datos en los que la categoria sea Soccer
  * 3. Guardar en txt
  * */
  implicit val sparkSession = Spark.createLocalSession
  //Nos creamos un schema para la cabecera
  val myschema= StructType(Array(
    StructField("id", StringType,true),
    StructField("departament_id", StringType,true),
    StructField("name", StringType,true)))
  implicit val categoriesDF: DataFrame = MainRead.readCsvSchema("src/main/resources/retail_db/categories/part-m-00000",myschema)
  //Comprobamos que se ha creado bien
  categoriesDF.show
  // Hacemos el filtro Soccer
  val soccersol = categoriesDF.where("name == 'Soccer'")
  //Vemos que esta bien
  soccersol.show()
  //Guardamos en txt
  soccersol.rdd.map(_.toString().replace("[","").replace("]", "")).saveAsTextFile("dataset/q4/solution")
}
