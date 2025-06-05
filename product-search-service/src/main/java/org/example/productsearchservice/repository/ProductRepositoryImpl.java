package org.example.productsearchservice.repository;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.bucket.nested.ParsedNested;
import org.elasticsearch.search.aggregations.bucket.nested.ParsedReverseNested;
import org.elasticsearch.search.aggregations.bucket.range.ParsedRange;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregator;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.example.productsearchservice.model.ProductAggregationDto;
import org.example.productsearchservice.model.ProductRequest;
import org.example.productsearchservice.model.ProductServiceResponse;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Repository
@Slf4j
@RequiredArgsConstructor
public class ProductRepositoryImpl implements ProductRepository {

    private static final String PRICE_AGG = "priceRangeAgg";
    private static final String SIZE_AGG = "sizeRangeAgg";
    private static final String COLOR_AGG = "colorRangeAgg";
    private static final String BRAND_AGG = "brandRangeAgg";
    private static final String NAME_FIELD = "name";
    private static final String NAME_SHINGLE = "name.shingles";
    private static final String BRAND_FIELD = "brand";
    private static final String BRAND_SHINGLE = "brand.shingles";
    private static final String BRAND_KEYWORD_FIELD = "brand.keyword";
    private static final String NESTED_SKU_SIZE_FIELD = "nested_skus_size";
    private static final String NESTED_SKU_COLOR_FIELD = "nested_skus_color";
    private static final String SKU_COLOR = "skus.color";
    private static final String COLOR = "color";
    private static final String SKU_SIZE = "skus.size";
    private static final String SKU_FIELD = "skus";
    private static final String SIZE = "size";
    private static final String PRICE = "price";
    private static final String ID_FIELD = "_id";
    private static final String SCORE_FIELD = "_score";
    private static final int AGGREGATION_SIZE = 1000;

    private final RestHighLevelClient esClient;

    @Value("${product.search.index}")
    private String aliasName;

    @Value("${product.search.request.fuzziness.startsFromLength.one:4}")
    int fuzzyOneStartsFromLength;
    @Value("${product.search.request.fuzziness.startsFromLength.two:6}")
    int fuzzyTwoStartsFromLength;
    @Value("${product.search.request.fuzziness.boost.zero:1.0}")
    float fuzzyZeroBoost;
    @Value("${product.search.request.fuzziness.boost.one:0.5}")
    float fuzzyOneBoost;
    @Value("${product.search.request.fuzziness.boost.two:0.25}")
    float fuzzyTwoBoost;
    @Value("${product.search.request.prefixQueryBoost:0.9}")
    float prefixQueryBoost;

    @Override
    public ProductServiceResponse getAllProductsByQuery(ProductRequest request) {
        QueryBuilder mainQuery = getQueryByText(request.getTextQuery());
        return getProducts(mainQuery, request);
    }

    private ProductServiceResponse getProducts(QueryBuilder mainQuery, ProductRequest request) {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
                .query(mainQuery)
                .from(request.getPage() * request.getSize())
                .size(request.getSize());

        searchSourceBuilder.sort(new FieldSortBuilder(SCORE_FIELD).order(SortOrder.DESC));
        searchSourceBuilder.sort(new FieldSortBuilder(ID_FIELD).order(SortOrder.DESC));

        List<AggregationBuilder> aggs = createAggs();
        aggs.forEach(searchSourceBuilder::aggregation);

        SearchRequest searchRequest = new SearchRequest(aliasName).source(searchSourceBuilder);
        try {
            SearchResponse searchResponse = esClient.search(searchRequest, RequestOptions.DEFAULT);
            return getServiceResponse(searchResponse);
        } catch (IOException ex) {
            log.error(ex.getMessage(), ex);
            return new ProductServiceResponse();
        }
    }

    private List<AggregationBuilder> createAggs() {
        //Price aggregation
        List<AggregationBuilder> result = new ArrayList<>();
        RangeAggregationBuilder itemCountAgg = AggregationBuilders
                .range(PRICE_AGG)
                .field(PRICE)
                .keyed(true)
                .addRange(new RangeAggregator.Range("Cheap", 0.0, 99.99))
                .addRange(new RangeAggregator.Range("Average", 100.0, 499.99))
                .addRange(new RangeAggregator.Range("Expensive", 500.0, null));

        result.add(itemCountAgg);

        //Brand aggregation
        TermsAggregationBuilder brandAggregation = AggregationBuilders.terms(BRAND_AGG)
                .field(BRAND_KEYWORD_FIELD)
                .size(AGGREGATION_SIZE)
                .order(BucketOrder.compound(
                        Arrays.asList(
                                BucketOrder.count(false),
                                BucketOrder.key(true)
                        )
                ));
        result.add(brandAggregation);

        //SKU aggregation
        AggregationBuilder skuSizeAggregation = AggregationBuilders.nested(NESTED_SKU_SIZE_FIELD, SKU_FIELD)
                .subAggregation(
                        AggregationBuilders.terms(SIZE_AGG)
                                .field(SKU_SIZE)
                                .size(AGGREGATION_SIZE)
                                .order(BucketOrder.compound(
                                        Arrays.asList(
                                                BucketOrder.aggregation("reverse_to_product.doc_count", false),
                                                BucketOrder.key(true)
                                        )
                                )).subAggregation(AggregationBuilders.reverseNested("reverse_to_product")));
        result.add(skuSizeAggregation);

        AggregationBuilder skuColorAggregation = AggregationBuilders.nested(NESTED_SKU_COLOR_FIELD, SKU_FIELD)
                .subAggregation(
                        AggregationBuilders.terms(COLOR_AGG)
                                .field(SKU_COLOR)
                                .size(AGGREGATION_SIZE)
                                .order(BucketOrder.compound(
                                        Arrays.asList(
                                                BucketOrder.aggregation("reverse_to_product.doc_count", false),
                                                BucketOrder.key(true)
                                        )
                                )
        )
                .subAggregation(AggregationBuilders.reverseNested("reverse_to_product")));
        result.add(skuColorAggregation);

        return result;
    }

    private ProductServiceResponse getServiceResponse(SearchResponse searchResponse) {
        ProductServiceResponse response = new ProductServiceResponse();

        response.setTotalHits(searchResponse.getHits().getTotalHits().value);

        List<Map<String, Object>> products = Arrays.stream(searchResponse.getHits().getHits())
                .map(SearchHit::getSourceAsMap)
                .collect(Collectors.toList());
        response.setProducts(products);

        mapPriceAggregation(searchResponse, response);
        mapBrandAggregation(searchResponse, response);
        mapSkuColorAggregation(searchResponse, response);
        mapSkuSizeAggregation(searchResponse, response);

        return response;
    }

    private static void mapPriceAggregation(SearchResponse searchResponse, ProductServiceResponse response) {
        ParsedRange parsedRange = searchResponse.getAggregations().get(PRICE_AGG);

        List<ProductAggregationDto> priceAggregationData = parsedRange.getBuckets().stream()
                .map(bucket -> ProductAggregationDto.builder()
                        .count(bucket.getDocCount())
                        .value(bucket.getKeyAsString())
                        .build())
                .collect(Collectors.toList());

        response.getFacets().put(PRICE, priceAggregationData);
    }

    private static void mapSkuColorAggregation(SearchResponse searchResponse, ProductServiceResponse response) {
        ParsedNested nested = searchResponse.getAggregations().get(NESTED_SKU_COLOR_FIELD);
        ParsedTerms colorTerms = nested.getAggregations().get(COLOR_AGG);

        List<ProductAggregationDto> colorFacet = colorTerms.getBuckets().stream()
                .map(bucket -> {
                    ParsedReverseNested reverseNested = bucket.getAggregations().get("reverse_to_product");
                    long productCount = reverseNested.getDocCount();
                    return ProductAggregationDto.builder()
                            .count(productCount)
                            .value(capitalize(bucket.getKeyAsString()))
                            .build();
                })
                .collect(Collectors.toList());

        response.getFacets().put(COLOR, colorFacet);
    }

    private static String capitalize(String s) {
        return s == null || s.isEmpty() ? s : Character.toUpperCase(s.charAt(0)) + s.substring(1);
    }

    private static void mapSkuSizeAggregation(SearchResponse searchResponse, ProductServiceResponse response) {
        ParsedNested nested = searchResponse.getAggregations().get(NESTED_SKU_SIZE_FIELD);
        ParsedTerms colorTerms = nested.getAggregations().get(SIZE_AGG);

        List<ProductAggregationDto> colorFacet = colorTerms.getBuckets().stream()
                .map(bucket -> {
                    ParsedReverseNested reverseNested = bucket.getAggregations().get("reverse_to_product");
                    long productCount = reverseNested.getDocCount();
                    return ProductAggregationDto.builder()
                            .count(productCount)
                            .value(bucket.getKeyAsString().toUpperCase())
                            .build();
                })
                .collect(Collectors.toList());

        response.getFacets().put(SIZE, colorFacet);
    }

    private static void mapBrandAggregation(SearchResponse searchResponse, ProductServiceResponse response) {
        Terms terms = searchResponse.getAggregations().get(BRAND_AGG);

        List<ProductAggregationDto> brandFacet = terms.getBuckets().stream()
                .map(bucket -> {
                    return ProductAggregationDto.builder()
                            .count(bucket.getDocCount())
                            .value(bucket.getKeyAsString())
                            .build();
                })
                .collect(Collectors.toList());

        response.getFacets().put(BRAND_FIELD, brandFacet);
    }

    private QueryBuilder getQueryByText(String textQuery) {
        String[] tokens = textQuery.toLowerCase().split("\\s+");
        List<String> sizes = Arrays.asList("xxs", "xs", "s", "m", "l", "xl", "xxl", "xxxl");
        List<String> colors = Arrays.asList("green", "black", "white", "blue", "yellow", "red", "brown", "orange", "grey");

        BoolQueryBuilder mainBoolQuery = QueryBuilders.boolQuery();
        List<String> generalTokens = new ArrayList<>();

        String matchedSize = null;
        String matchedColor = null;

        for (String token : tokens) {
            if (sizes.contains(token)) {
                matchedSize = token;
            } else if (colors.contains(token)) {
                matchedColor = token;
            } else {
                generalTokens.add(token);
            }
        }

        if (matchedColor != null || matchedSize != null) {
            BoolQueryBuilder nestedSkuQuery = QueryBuilders.boolQuery();
            if (matchedColor != null) {
                nestedSkuQuery.must(QueryBuilders.matchQuery(SKU_COLOR, matchedColor));
            }
            if (matchedSize != null) {
                nestedSkuQuery.must(QueryBuilders.matchQuery(SKU_SIZE, matchedSize));
            }

            mainBoolQuery.must(QueryBuilders.nestedQuery(SKU_FIELD, nestedSkuQuery, ScoreMode.Avg));
        }

        if (!generalTokens.isEmpty()) {
            String generalQueryText = String.join(" ", generalTokens);

            // Multi-match query over name and brand fields
            MultiMatchQueryBuilder crossFieldQuery = QueryBuilders.multiMatchQuery(
                            generalQueryText,
                            NAME_FIELD, BRAND_FIELD
                    )
                    .type(MultiMatchQueryBuilder.Type.CROSS_FIELDS)
                    .operator(Operator.AND);

            mainBoolQuery.must(crossFieldQuery);

            // Shingles boost query
            MultiMatchQueryBuilder shinglesBoostQuery = QueryBuilders.multiMatchQuery(
                            generalQueryText,
                            NAME_SHINGLE, BRAND_SHINGLE
                    )
                    .type(MultiMatchQueryBuilder.Type.PHRASE)
                    .boost(5);

            mainBoolQuery.should(shinglesBoostQuery);
        }

        return mainBoolQuery;
    }

    private int getDistanceByTermLength(final String token) {
        return token.length() >= fuzzyTwoStartsFromLength
                ? 2
                : (token.length() >= fuzzyOneStartsFromLength ? 1 : 0);
    }

    private float getBoostByDistance(final int distance) {
        return distance == 0
                ? fuzzyZeroBoost
                : (distance == 1 ? fuzzyOneBoost : fuzzyTwoBoost);
    }
}
