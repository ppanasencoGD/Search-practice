package org.example.productsearchservice.rest;

import org.example.productsearchservice.model.ProductRequest;
import org.example.productsearchservice.model.ProductServiceResponse;
import org.example.productsearchservice.service.ProductServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(value = "/v1/products")
public class ProductController {

    @Autowired
    private ProductServiceImpl productService;

    @PostMapping(consumes = MediaType.APPLICATION_JSON_VALUE,
        produces = MediaType.APPLICATION_JSON_VALUE)
    public ProductServiceResponse getSearchServiceResponse(@RequestBody ProductRequest request) {
        return productService.getServiceResponse(request);
    }
}
