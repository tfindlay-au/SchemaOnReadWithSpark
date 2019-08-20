package MyDataProducts.raw

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions.from_json

/**
 * This object provides raw access to data stored on disk.
 * This pattern could also wrap any source of data including Kafka or RDBMS as long as it returns a DataFrame.
 *
 * @example <pre>
 * z.load("/path/to.jar")
 * import MyDataProducts.raw.ProductRawMenus
 * val spark = SparkSession.builder.getOrCreate()
 * import spark.implicits._
 * val df = spark.productRawMenus()
 * </pre>
 */
object ProductRawMenus {

  /**
   * This holds each line item on the menu
   * {{{ Parent: Menu }}}
   * @param item_type Text category of the item. eg. Desert, Main, Drink etc
   * @param name Test description of the item
   * @param price Standard price of the item
   */
  case class MenuItem(
                      item_type: String,
                      name: String,
                      price: Float
                      )

  /**
   * This is a wrapper for MenuItem and may have different types of menus for each restaurant
   * {{{ Parent: OutputData }}}
   * @param menu_items Array of MenuItem objects
   */
  case class Menu(
                   menu_items: Array[MenuItem]
                 )

  /**
   * This is a simple wrapper for a Menu used to parse the packed JSON string
   * @note This is just because of my poor design, dont design structures like this!
   * @param menu A Single menu containing menu_items
   */
  case class MenuField(
                      menu: Menu
                      )

  /**
   * This is the main container for each restaurant
   * @param restaurant The name of the restaurant
   * @param menu Structure containing menu details
   */
  case class OutputData(
                    restaurant: String,
                    menu: Menu
                    )

  /**
   * This is the main transformation access logic and typically contains minimal transformation
   * @param sparkSession Apache Spark session to use
   * @return Dataset in a hierarchy of OutputData
   */
  def transform(sparkSession: SparkSession): Dataset[OutputData] = {
    import sparkSession.implicits._

    // This is configuration contains the location of the source data
    val inputPath = "/data/parquet"

    // Simpler to use reflection on the case class then duplicate the schema definition as Spark Structs
    val rawSchema = ScalaReflection.schemaFor[MenuField].dataType.asInstanceOf[StructType]

    // Use Apache Spark to read the data (lazily) and return a Dataset with named elements
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