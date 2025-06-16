package org.example.productsearchservice;

import org.example.productsearchservice.service.ProductServiceImpl;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import static org.hamcrest.Matchers.*;

public class ExercisesIntegrationTest extends BaseTest {

    private final APIClient client = new APIClient();

    @Autowired
    ProductServiceImpl productService;

    @Before
    public void init() throws InterruptedException {
        productService.recreateIndex();
        Thread.sleep(1100);
    }

    @Test
    public void getEmptyResponseTest() {
        client
                .productRequest()
                .body("{}")
                .post()
                .then()
                .statusCode(200)
                .body("totalHits", is(0));
    }

    @Test
    public void testQueryReturnsEmptyForWrongWord() {
        client.productRequest()
                .body("{\"textQuery\": \"Calvin klein L blue ankle skinny jeans wrongword\"}")
                .post()
                .then()
                .statusCode(200)
                .body("totalHits", is(0));
    }

    @Test
    public void testQueryReturnsEmptyForNoMatch() {
        client.productRequest()
                .body("{\"textQuery\": \"Calvin klein L red ankle skinny jeans\"}")
                .post()
                .then()
                .statusCode(200)
                .body("totalHits", is(0));
    }

    @Test
    public void testHappyPathReturnsCorrectProduct() {
        client.productRequest()
                .body("{\"textQuery\": \"Calvin klein L blue ankle skinny jeans\"}")
                .post()
                .then()
                .statusCode(200)
                .body("totalHits", is(1))
                .body("products", hasSize(1))
                .body("products[0].id", is("2"))
                .body("products[0].brand", is("Calvin Klein"))
                .body("products[0].name", is("Women ankle skinny jeans, model 1282"))
                .body("products[0].skus", hasSize(9))
                .body("facets.brand", notNullValue())
                .body("facets.price", notNullValue())
                .body("facets.color", notNullValue())
                .body("facets.size", notNullValue());
    }

    @Test
    public void testFacetsJeans() {
        client.productRequest()
                .body("{\"textQuery\": \"jeans\"}")
                .post()
                .then()
                .statusCode(200)

                // brand facets
                .body("facets.brand", hasSize(2))
                .body("facets.brand[0].value", is("Calvin Klein"))
                .body("facets.brand[0].count", is(4))
                .body("facets.brand[1].value", is("Levi's"))
                .body("facets.brand[1].count", is(4))

                // price facets
                .body("facets.price", hasSize(3))
                .body("facets.price[0].value", is("Cheap"))
                .body("facets.price[0].count", is(2))
                .body("facets.price[1].value", is("Average"))
                .body("facets.price[1].count", is(6))
                .body("facets.price[2].value", is("Expensive"))
                .body("facets.price[2].count", is(0))

                // color facets
                .body("facets.color", hasSize(4))
                .body("facets.color[0].value", is("Blue"))
                .body("facets.color[0].count", is(8))
                .body("facets.color[1].value", is("Black"))
                .body("facets.color[1].count", is(7))
                .body("facets.color[2].value", is("Red"))
                .body("facets.color[2].count", is(1))
                .body("facets.color[3].value", is("White"))
                .body("facets.color[3].count", is(1))

                // size facets
                .body("facets.size", hasSize(6))
                .body("facets.size[0].value", is("L"))
                .body("facets.size[0].count", is(8))
                .body("facets.size[1].value", is("M"))
                .body("facets.size[1].count", is(8))
                .body("facets.size[2].value", is("S"))
                .body("facets.size[2].count", is(6))
                .body("facets.size[3].value", is("XL"))
                .body("facets.size[3].count", is(5))
                .body("facets.size[4].value", is("XXL"))
                .body("facets.size[4].count", is(3))
                .body("facets.size[5].value", is("XS"))
                .body("facets.size[5].count", is(2));
    }

    @Test
    public void testFacetsWomenAnkleBlueJeans() {
        client.productRequest()
                .body("{\"textQuery\": \"women ankle blue jeans\"}")
                .post()
                .then()
                .statusCode(200)

                // check all 4 facets are present
                .body("facets.brand", notNullValue())
                .body("facets.price", notNullValue())
                .body("facets.color", notNullValue())
                .body("facets.size", notNullValue())

                // brand facets
                .body("facets.brand", hasSize(2))
                .body("facets.brand[0].value", is("Calvin Klein"))
                .body("facets.brand[0].count", is(2))
                .body("facets.brand[1].value", is("Levi's"))
                .body("facets.brand[1].count", is(1))

                // price facets
                .body("facets.price", hasSize(3))
                .body("facets.price[0].value", is("Cheap"))
                .body("facets.price[0].count", is(0))
                .body("facets.price[1].value", is("Average"))
                .body("facets.price[1].count", is(3))
                .body("facets.price[2].value", is("Expensive"))
                .body("facets.price[2].count", is(0))

                // color facets
                .body("facets.color", hasSize(4))
                .body("facets.color[0].value", is("Black"))
                .body("facets.color[0].count", is(3))
                .body("facets.color[1].value", is("Blue"))
                .body("facets.color[1].count", is(3))
                .body("facets.color[2].value", is("Red"))
                .body("facets.color[2].count", is(1))
                .body("facets.color[3].value", is("White"))
                .body("facets.color[3].count", is(1))

                // size facets
                .body("facets.size", hasSize(4))
                .body("facets.size[0].value", is("L"))
                .body("facets.size[0].count", is(3))
                .body("facets.size[1].value", is("M"))
                .body("facets.size[1].count", is(3))
                .body("facets.size[2].value", is("S"))
                .body("facets.size[2].count", is(3))
                .body("facets.size[3].value", is("XS"))
                .body("facets.size[3].count", is(1));
    }

    @Test
    public void testBoostedSortingWithTextQueryJeans() {
        client.productRequest()
                .body("{\"textQuery\": \"jeans\"}")
                .post()
                .then()
                .statusCode(200)
                .body("totalHits", is(8))
                .body("products", hasSize(8))
                .body("products[0].id", is("8"))
                .body("products[1].id", is("7"))
                .body("products[2].id", is("6"))
                .body("products[3].id", is("5"))
                .body("products[4].id", is("4"))
                .body("products[5].id", is("3"))
                .body("products[6].id", is("2"))
                .body("products[7].id", is("1"));
    }

    @Test
    public void testBoostedSortingWithTextQueryBlueWomenJeans() {
        client.productRequest()
                .body("{\"textQuery\": \"blue WOMEN jeans\"}")
                .post()
                .then()
                .statusCode(200)
                .body("totalHits", is(5))
                .body("products", hasSize(5))
                .body("products[0].id", is("5"))
                .body("products[1].id", is("3"))
                .body("products[2].id", is("6"))
                .body("products[3].id", is("2"))
                .body("products[4].id", is("1"));
    }

    @Test
    public void testBoostedSortingWithTextQueryWomenBlueJeans() {
        client.productRequest()
                .body("{\"textQuery\": \"WOMEN blue jeans\"}")
                .post()
                .then()
                .statusCode(200)
                .body("totalHits", is(5))
                .body("products", hasSize(5))
                .body("products[0].id", is("6"))
                .body("products[1].id", is("5"))
                .body("products[2].id", is("3"))
                .body("products[3].id", is("2"))
                .body("products[4].id", is("1"));
    }

    @Test
    public void testBoostedSortingWithTextQueryWomenAnkleBlueJeans() {
        client.productRequest()
                .body("{\"textQuery\": \"women ankle blue jeans\"}")
                .post()
                .then()
                .statusCode(200)
                .body("totalHits", is(3))
                .body("products", hasSize(3))
                .body("products[0].id", is("6"))
                .body("products[1].id", is("2"))
                .body("products[2].id", is("1"));
    }

    @Test
    public void testPaginationWithSizeAndPage() {
        client.productRequest()
                .body("{\"textQuery\": \"jeans\", \"size\": 2, \"page\": 1}")
                .post()
                .then()
                .statusCode(200)
                .body("totalHits", is(8))
                .body("products", hasSize(2))
                .body("products[0].id", is("6"))
                .body("products[1].id", is("5"));
    }
}
