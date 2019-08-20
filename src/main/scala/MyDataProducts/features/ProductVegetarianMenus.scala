package MyDataProducts.features

import MyDataProducts.raw.ProductRawMenus
import MyDataProducts.raw.ProductRawMenus.OutputData
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.explode

/**
 * This object provides a defined feature for a use case.
 * It would typically involved leveraging a `raw` product and applying enrichment's and filters often returning a
 * different structure specifically for a given use case
 *
 * @example <pre>
 *          z.load("/path/to.jar")
 *          import MyDataProducts.raw.ProductRawMenus
 *          val spark = SparkSession.builder.getOrCreate()
 *          import spark.implicits._
 *          val df = spark.ProductVegetarianMenus()
 *          </pre>
 */
object ProductVegetarianMenus {

  /**
   * The transformation logic is applied here...
   * @param sparkSession Spark instance to used
   * @return Dataset of OutputData or you could define a new structure in this object
   */
  def transform(sparkSession: SparkSession): Dataset[OutputData] = {
    import sparkSession.implicits._

    // Leverage the logic to acquire the raw data
    ProductRawMenus.transform(sparkSession)
      // This is a horrible way to do this, dont ever do things like this
      .withColumn("item_name", explode($"menu.menu_items.name"))
      .where($"item_name".notEqual( "Ojo de bife"))
      .drop("item_name")
      .as[OutputData]
  }

  implicit class SparkSessionWithProductVegetarianMenus(sparkSession: SparkSession) {
    def productVegetarianMenus(): Dataset[OutputData] = {
      transform(sparkSession)
    }
  }

}
