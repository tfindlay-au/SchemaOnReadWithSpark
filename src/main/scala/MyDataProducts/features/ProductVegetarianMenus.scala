package MyDataProducts.features

import MyDataProducts.raw.ProductRawMenus
import MyDataProducts.raw.ProductRawMenus.OutputData
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.explode

object ProductVegetarianMenus {
  def transform(sparkSession: SparkSession): Dataset[OutputData] = {
    import sparkSession.implicits._

    ProductRawMenus.transform(sparkSession)
      .withColumn("item_name", explode($"menu.menu_items.name"))
      .where($"item_name".notEqual( "Ojo de bife"))
      .drop("item_name")
      .as[OutputData]
  }

  implicit class SparkSessionWithProductVegetarianMenus(sparkSession: SparkSession) {
    def ProductVegetarianMenus(): Dataset[OutputData] = {
      transform(sparkSession)
    }
  }

}
