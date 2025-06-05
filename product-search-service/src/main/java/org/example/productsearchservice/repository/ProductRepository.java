package org.example.productsearchservice.repository;

import org.example.productsearchservice.model.ProductRequest;
import org.example.productsearchservice.model.ProductServiceResponse;

public interface ProductRepository {
    ProductServiceResponse getAllProductsByQuery(ProductRequest request);
}
