package com.vengat.tuts.datagenerator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.vengat.tuts.types.LineItem;

import java.io.File;
import java.io.IOException;
import java.util.Random;

public class ProductGenerator {

    private static final ProductGenerator productGenerator = new ProductGenerator();
    private final Random random;
    private final Random qty;
    private LineItem[] products;

    public static ProductGenerator getInstance() {
        return productGenerator;
    }

    public ProductGenerator() {
        random = new Random();
        qty = new Random();
        ObjectMapper objectMapper = new ObjectMapper();

        try {
            products = objectMapper.readValue(new File("src/main/resources/data/products.json"), LineItem[].class);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private int getIndex() {
        return random.nextInt(100);
    }

    private int getQuantity() {
        return qty.nextInt(2) + 1;
    }

    public LineItem getNextProduct() {
        LineItem lineItem = products[getIndex()];
        lineItem.setItemQty(getQuantity());
        lineItem.setTotalValue(lineItem.getItemPrice() * lineItem.getItemQty());
        return lineItem;
    }
}
