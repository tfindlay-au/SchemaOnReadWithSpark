package MyDataProducts.raw

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions.from_json

/**
 * z.load("/path/to.jar")
 * import MyDataProducts.raw.ProductRawMenus
 * val spark = SparkSession.builder.getOrCreate()
 * import spark.implicits._
 * val df = spark.productRawMenus()
 *
 */
object ProductRawMenus {
  case class InputData(
                        name: String,
                        menu: String
                        )

  case class MenuItem(
                      item_type: String,
                      name: String,
                      price: Float
                      )
  case class Menu(
                   menu_items: Array[MenuItem]
                 )

  case class MenuField(
                      menu: Menu
                      )

  case class OutputData(
                    restaurant: String,
                    menu: Menu
                    )

  def transform(sparkSession: SparkSession): Dataset[OutputData] = {
    import sparkSession.implicits._

    val inputPath = "/data/parquet"

    val rawSchema = ScalaReflection.schemaFor[MenuField].dataType.asInstanceOf[StructType]

    sparkSession.read
      .format("parquet")
      .load(inputPath)
      .withColumnRenamed("name", "restaurant")
      .withColumn("raw", from_json($"menu", rawSchema))
      .select("restaurant", "raw.menu")
      .as[OutputData]
  }

  implicit class SparkSessionWithProductRawMenus(sparkSession: SparkSession) {
    def productRawMenus(): Dataset[OutputData] = {
      transform(sparkSession)
    }
  }

}