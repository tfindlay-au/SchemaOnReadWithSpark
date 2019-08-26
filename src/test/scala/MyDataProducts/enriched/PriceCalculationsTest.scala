package MyDataProducts.enriched

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{ArrayType, FloatType, StringType, StructField, StructType}
import org.scalatest.MustMatchers._
import org.scalatest.{Matchers, WordSpec}

object PriceCalculationsTest {

}

class PriceCalculationsTest extends WordSpec with Matchers with SharedSparkContext {
//  override def withFixture(test: NoArgTest) = { // Define a shared fixture
//    val mySchema = StructType(List(
//      StructField("restaurant", StringType, nullable = true),
//      StructField("menu", StructType(List(
//        StructField("menu_items", ArrayType(StructType(List(
//          StructField("item_type", StringType, nullable = true),
//          StructField("name", StringType, nullable = true),
//          StructField("price", FloatType, nullable = true)
//        ))))
//      )))
//    ))
//
//    // Shared setup (run at beginning of each test)
//    try test(mySchema)
//    finally {
//      // Shared cleanup (run at end of each test)
//    }
//  }


  "GSTFunction" must {
    "add column 'priceWithGST'" in {
      // TODO Move to setup function
      val spark = SparkSession.builder.getOrCreate()
      import spark.implicits._

      val mySchema = StructType(List(
        StructField("restaurant", StringType, nullable = true),
        StructField("menu", StructType(List(
          StructField("menu_items", ArrayType(StructType(List(
            StructField("item_type", StringType, nullable = true),
            StructField("name", StringType, nullable = true),
            StructField("price", FloatType, nullable = true)
          ))))
        )))
      ))

      // Given an input data frame ...
      val inputData = Seq(Row("TestRestaurant", "{\"menu\": { \"menu_items\": [  {\"item_type\": \"TestItem\", \"name\": \"TestName\", \"price\": 100.0 }}" ))
      val inputDataFrame = spark.createDataFrame(spark.sparkContext.parallelize(inputData), mySchema)

      // When we call the GST function ...
      val resultDataFrame = PriceCalculations.GSTFunction(inputDataFrame)

      // Then the result should be ...
      val expectedDataFrame = Seq(
        ("TestRestaurant", "{\"menu\": { \"menu_items\": [  {\"item_type\": \"TestItem\", \"name\": \"TestName\", \"price\": 100.0 }]}, \"priceWithGST\": 120.0 }" )
      ).toDF()
//      val expectedData = Seq(Row("TestRestaurant", "{\"menu\": { \"menu_items\": [  {\"item_type\": \"TestItem\", \"name\": \"TestName\", \"price\": 110.0 }}" ))
//      val expectedDataFrame = spark.createDataFrame(spark.sparkContext.parallelize(expectedData), mySchema)

      // Use import com.holdenkarau.spark.testing.DataFrameSuiteBase
      // with DataFrameSuiteBase
      //      assertDataFrameEquals(expectedDataFrame, resultDataFrame)
      resultDataFrame should be eq(expectedDataFrame)

    }

    "Still work if the menu.menu_items.price is missing" in {

    }
  }

  "CreditCardFeesFunction" must {

  }
}
