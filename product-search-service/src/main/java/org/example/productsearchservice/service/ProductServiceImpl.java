package org.example.productsearchservice.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.GetAliasesResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.example.productsearchservice.model.ProductRequest;
import org.example.productsearchservice.model.ProductServiceResponse;
import org.example.productsearchservice.repository.ProductRepository;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;

@Service
@RequiredArgsConstructor
@Slf4j
public class ProductServiceImpl {

    private final ProductRepository productRepository;
    private final RestHighLevelClient esClient;
    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Value("${product.search.request.default.page}")
    private int defaultPage;
    @Value("${product.search.request.default.size}")
    private int defaultSize;
    @Value("${product.search.request.minQueryLength}")
    private int minQueryLength;

    @Value("${product.search.files.mappings:classpath:elastic/product/mappings.json}")
    private Resource productsMappingsFile;
    @Value("${product.search.files.settings:classpath:elastic/product/settings.json}")
    private Resource productsSettingsFile;
    @Value("${product.search.files.bulkData:classpath:elastic/product/task_8_data.json}")
    private Resource productsBulkInsertDataFile;
    @Value("${product.search.index}")
    private String aliasName;

    private static final int MAX_INDICES_NUMBER = 3;

    public void recreateIndex() {
        String settings = getStrFromResource(productsSettingsFile);
        String mappings = getStrFromResource(productsMappingsFile);

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("_yyyyMMddHHmmss");
        String timestamp = LocalDateTime.now().format(formatter);
        String newIndexName = aliasName + timestamp;

        createIndex(newIndexName, settings, mappings);
        updateAliasesByName(aliasName, newIndexName);
        deleteOutdatedIndex(aliasName, newIndexName);

        processBulkInsertData(productsBulkInsertDataFile, newIndexName);
    }

    private void updateAliasesByName(String aliasName, String newIndexName) {
        GetAliasesResponse aliasesResponse;
        try {
            IndicesAliasesRequest indicesAliasesRequest = new IndicesAliasesRequest();
            GetAliasesRequest getAliasesRequest = new GetAliasesRequest()
                    .aliases(aliasName);
            aliasesResponse = esClient.indices().getAlias(getAliasesRequest, RequestOptions.DEFAULT);

            aliasesResponse.getAliases().keySet().forEach(existingIndex -> {
                IndicesAliasesRequest.AliasActions updateAliasActions = new IndicesAliasesRequest.AliasActions(IndicesAliasesRequest.AliasActions.Type.REMOVE)
                        .index(existingIndex).alias(aliasName);
                indicesAliasesRequest.addAliasAction(updateAliasActions);
            });

            IndicesAliasesRequest.AliasActions addAliasActions = new IndicesAliasesRequest.AliasActions(IndicesAliasesRequest.AliasActions.Type.ADD)
                    .index(newIndexName).alias(aliasName);
            indicesAliasesRequest.addAliasAction(addAliasActions);

            AcknowledgedResponse aliasUpdateResponse = esClient.indices().updateAliases(indicesAliasesRequest, RequestOptions.DEFAULT);
            if (!aliasUpdateResponse.isAcknowledged()) {
                throw new RuntimeException("Alias update not acknowledged.");
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to update aliases for: " + newIndexName, e);
        }
    }

    private void deleteOutdatedIndex(String aliasName, String newIndexName) {
        try {
            GetIndexRequest getIndexRequest = new GetIndexRequest(aliasName + "_*");
            String[] allIndices = esClient.indices().get(getIndexRequest, RequestOptions.DEFAULT).getIndices();

            List<String> matchingIndices = Arrays.stream(allIndices)
                    .sorted(Comparator.reverseOrder())
                    .skip(MAX_INDICES_NUMBER)
                    .collect(Collectors.toList());

            if (!matchingIndices.isEmpty()) {
                DeleteIndexRequest deleteRequest = new DeleteIndexRequest(matchingIndices.toArray(new String[0]));
                AcknowledgedResponse acknowledgedResponse = esClient.indices().delete(deleteRequest, RequestOptions.DEFAULT);
                if (!acknowledgedResponse.isAcknowledged()) {
                    throw new RuntimeException("Index delete not acknowledged.");
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to delete aliases for: " + newIndexName, e);
        }
    }

    private void processBulkInsertData(Resource bulkInsertDataFile, String newIndexName) {
        try {
            BulkRequest bulkRequest = new BulkRequest();
            bulkRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);

            List<Map<String, Object>> products = objectMapper.readValue(
                    bulkInsertDataFile.getInputStream(),
                    new TypeReference<List<Map<String, Object>>>() {
                    }
            );

            products.stream()
                    .map(product -> {
                        String id = product.get("id").toString();
                        return new IndexRequest(newIndexName)
                                .id(id)
                                .source(product);
                    })
                    .forEach(bulkRequest::add);

            BulkResponse bulkResponse = esClient.bulk(bulkRequest, RequestOptions.DEFAULT);
            if (bulkResponse.getItems().length != products.size()) {
                log.warn("Only {} out of {} requests have been processed in a bulk request.", bulkResponse.getItems().length, products.size());
            } else {
                log.info("{} requests have been processed in a bulk request.", bulkResponse.getItems().length);
            }

            if (bulkResponse.hasFailures()) {
                log.warn("Bulk data processing has failures:\n{}", bulkResponse.buildFailureMessage());
            }

        } catch (IOException ex) {
            log.error("An exception occurred during bulk data processing", ex);
            throw new RuntimeException(ex);
        }
    }

    private void createIndex(String newIndexName, String settings, String mappings) {
        CreateIndexRequest createIndexRequest = new CreateIndexRequest(newIndexName)
                .mapping(mappings, XContentType.JSON)
                .settings(settings, XContentType.JSON)
                .alias(new Alias(aliasName));

        CreateIndexResponse createIndexResponse;
        try {
            createIndexResponse = esClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
        } catch (IOException ex) {
            throw new RuntimeException("An error occurred during creating ES index.", ex);
        }

        if (!createIndexResponse.isAcknowledged()) {
            throw new RuntimeException("Creating index not acknowledged for newIndexName: " + newIndexName);
        } else {
            log.info("Index {} has been created.", newIndexName);
        }
    }

    private static String getStrFromResource(Resource resource) {
        try {
            if (!resource.exists()) {
                throw new IllegalArgumentException("File not found: " + resource.getFilename());
            }
            return Resources.toString(resource.getURL(), Charsets.UTF_8);
        } catch (IOException ex) {
            throw new IllegalArgumentException("Can not read resource file: " + resource.getFilename(), ex);
        }
    }

    public ProductServiceResponse getServiceResponse(ProductRequest request) {
        if (request.getTextQuery() == null || request.getTextQuery().isEmpty() || request.getTextQuery().length() < minQueryLength) {
            return ProductServiceResponse.builder()
                    .totalHits(0L)
                    .products(emptyList())
                    .facets(emptyMap())
                    .build();
        }
        prepareServiceRequest(request);
        return productRepository.getAllProductsByQuery(request);
    }

    private void prepareServiceRequest(ProductRequest request) {
        if (request.getSize() == null || request.getSize() <= 0) {
            request.setSize(defaultSize);
        }
        if (request.getPage() == null || request.getPage() <= 0) {
            request.setPage(defaultPage);
        }
    }
}
