package org.example.productindexer;

import org.example.productindexer.service.ProductService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.List;

import static java.util.Arrays.asList;

@SpringBootApplication
public class ProductIndexerApplication implements CommandLineRunner {

    private static final String RECREATE_INDEX_ARG = "recreateIndex";

    @Autowired
    ProductService productService;

    public static void main(String[] args) {
        SpringApplication.run(ProductIndexerApplication.class, args);
    }

    @Override
    public void run(String... strings) {
        List<String> args = asList(strings);
        boolean needRecreateIndex = args.contains(RECREATE_INDEX_ARG);
        if (true) {
            productService.recreateIndex();
        }
    }
}
