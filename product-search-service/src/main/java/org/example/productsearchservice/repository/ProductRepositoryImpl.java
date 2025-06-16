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
import org.elasticsearch.search.aggregations.bucket.nested.NestedAggregationBuilder;
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
import org.elasticsearch.search.sort.SortBuilders;
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

        searchSourceBuilder.sort(SortBuilders.scoreSort());
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
        List<AggregationBuilder> result = new ArrayList<>();
        RangeAggregationBuilder itemCountAgg = AggregationBuilders
                .range(PRICE_AGG)
                .field(PRICE)
                .keyed(true)
                .addRange(new RangeAggregator.Range("Cheap", 0.0, 99.99))
                .addRange(new RangeAggregator.Range("Average", 100.0, 499.99))
                .addRange(new RangeAggregator.Range("Expensive", 500.0, null));

        result.add(itemCountAgg);

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

        BucketOrder order = BucketOrder.compound(
                BucketOrder.aggregation("to_product.doc_count", false),
                BucketOrder.key(true)
        );

        TermsAggregationBuilder skuSizeAgg = AggregationBuilders.terms(NESTED_SKU_SIZE_FIELD)
                .field(SKU_SIZE)
                .order(order)
                .size(100)
                .shardSize(500)
                .subAggregation(
                        AggregationBuilders.reverseNested("to_product")
                );

        TermsAggregationBuilder skuColorAgg = AggregationBuilders.terms(NESTED_SKU_COLOR_FIELD)
                .field(SKU_COLOR)
                .order(order)
                .size(100)
                .shardSize(500)
                .subAggregation(
                        AggregationBuilders.reverseNested("to_product")
                );

        NestedAggregationBuilder nestedSkuAgg = AggregationBuilders.nested("agg_skus", SKU_FIELD)
                .subAggregation(skuSizeAgg)
                .subAggregation(skuColorAgg);

        result.add(nestedSkuAgg);

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
        ParsedNested nested = searchResponse.getAggregations().get("agg_skus");
        ParsedTerms colorTerms = nested.getAggregations().get(NESTED_SKU_COLOR_FIELD);

        List<ProductAggregationDto> colorFacet = colorTerms.getBuckets().stream()
                .map(bucket -> {
                    ParsedReverseNested reverseNested = bucket.getAggregations().get("to_product");
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
        ParsedNested nested = searchResponse.getAggregations().get("agg_skus");
        ParsedTerms colorTerms = nested.getAggregations().get(NESTED_SKU_SIZE_FIELD);

        List<ProductAggregationDto> colorFacet = colorTerms.getBuckets().stream()
                .map(bucket -> {
                    ParsedReverseNested reverseNested = bucket.getAggregations().get("to_product");
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
                .map(bucket -> ProductAggregationDto.builder()
                        .count(bucket.getDocCount())
                        .value(bucket.getKeyAsString())
                        .build())
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
                nestedSkuQuery.must(QueryBuilders.matchQuery(SKU_COLOR, matchedColor)).boost(3.0f);
            }
            if (matchedSize != null) {
                nestedSkuQuery.must(QueryBuilders.matchQuery(SKU_SIZE, matchedSize)).boost(2.0f);
            }

            mainBoolQuery.must(QueryBuilders.nestedQuery(SKU_FIELD, nestedSkuQuery, ScoreMode.Avg));
        }

        if (!generalTokens.isEmpty()) {
            String generalQueryText = String.join(" ", generalTokens);

            MultiMatchQueryBuilder crossFieldQuery = QueryBuilders.multiMatchQuery(
                            generalQueryText,
                            NAME_FIELD, BRAND_FIELD
                    )
                    .type(MultiMatchQueryBuilder.Type.CROSS_FIELDS)
                    .operator(Operator.AND);

            mainBoolQuery.must(crossFieldQuery);
        }

        MultiMatchQueryBuilder shinglesBoostQuery = QueryBuilders.multiMatchQuery(
                        textQuery,
                        NAME_SHINGLE, BRAND_SHINGLE
                )
                .type(MultiMatchQueryBuilder.Type.BEST_FIELDS)
                .boost(5.0f);

        mainBoolQuery.should(shinglesBoostQuery);

        return mainBoolQuery;
    }
}
