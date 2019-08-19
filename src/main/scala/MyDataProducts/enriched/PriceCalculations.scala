package MyDataProducts.enriched

import org.apache.spark.sql.DataFrame

object PriceCalculations {

  def GSTFunction(df: DataFrame): DataFrame = {
    import df.sparkSession.implicits._
    import org.apache.spark.sql.functions.explode

    df.withColumn("original_price", explode($"menu.menu_items.price"))
      .withColumn("priceWithGST", $"original_price" * 1.1)
      .drop("original_price")
  }

  def CreditCardFeesFunction(df: DataFrame): DataFrame = {
    import df.sparkSession.implicits._
    df.withColumn("priceWithCredCard", $"menu.menu_items.price" * 0.012)
  }

  implicit class SparkSessionWithPriceCalculations(df: DataFrame) {
    def withGST(): DataFrame = {
      GSTFunction(df)
    }

    def withCreditCardFees(): DataFrame = {
      CreditCardFeesFunction(df)
    }
  }

}
