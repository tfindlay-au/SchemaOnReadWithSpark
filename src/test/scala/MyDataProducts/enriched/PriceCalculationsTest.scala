package MyDataProducts.enriched

import org.scalatest.MustMatchers._
import org.scalatest.{Matchers, WordSpec}
import org.scalatest.mockito.MockitoSugar

object PriceCalculationsTest {

}

class PriceCalculationsTest extends WordSpec with Matchers with MockitoSugar {
  "GSTFunction" must {
    "add column 'priceWithGST'" in {

    }

    "Still work if the menu.menu_items.price is missing" in {

    }
  }

  "CreditCardFeesFunction" must {

  }
}
