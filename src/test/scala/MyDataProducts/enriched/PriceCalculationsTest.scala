package MyDataProducts.enriched

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{ArrayType, FloatType, StringType, StructField, StructType}
import org.scalatest.MustMatchers._
import org.scalatest.{Matchers, WordSpec}

object PriceCalculationsTest {

}

class PriceCalculationsTest extends WordSpec with Matchers with SharedSparkContext  {
  "GSTFunction" must {
    "add column 'priceWithGST'" in {
      // TODO Move to setup function
      val spark = SparkSession.builder.getOrCreate()

      val mySchema = StructType(List(
        StructField("restaurant", StringType, nullable = true),
        StructField("menu", StructType(List(
          StructField("menu_items", ArrayType(StructType(List(,
            StructField("item_type", StringType, nullable = true),
            StructField("name", StringType, nullable = true),
            StructField("price", FloatType, nullable = true)
          ))))
        )))
      ))

      // Given an input dataframe
      val inputData = Seq(Row("TestRestaurant", "{\"menu\": { \"menu_items\": [  {\"item_type\": \"TestItem\", \"name\": \"TestName\", \"price\": 100.0 }}" ))
      val input = spark.createDataFrame(spark.sparkContext.parallelize(inputData), mySchema)

      // When
      val result = PriceCalculations.GSTFunction(input)

      // Then
      val expectedData = Seq(Row("TestRestaurant", "{\"menu\": { \"menu_items\": [  {\"item_type\": \"TestItem\", \"name\": \"TestName\", \"price\": 110.0 }}" ))
      val expected = spark.createDataFrame(spark.sparkContext.parallelize(expectedData), mySchema)
      result should be eq(expected)

      // Use com.github.mrpowers.spark.fast.tests.DataFrameComparer ?
      // libraryDependencies += "com.github.mrpowers" %% "spark-fast-tests" % "2.3.0_0.11.0"
      //assertSmallDataFrameEquality(result, expected)
    }

    "Still work if the menu.menu_items.price is missing" in {

    }
  }

  "CreditCardFeesFunction" must {

  }
}
