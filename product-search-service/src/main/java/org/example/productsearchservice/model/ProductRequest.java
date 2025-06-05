package org.example.productsearchservice.model;

import lombok.Data;

@Data
public class ProductRequest {
    private String textQuery;
    private Integer page;
    private Integer size;
}
