package com.vengat.tuts.datagenerator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.vengat.tuts.types.DeliveryAddress;

import java.io.File;
import java.io.IOException;
import java.util.Random;

public class AddressGenerator {

    private static final AddressGenerator addressGenerator = new AddressGenerator();
    private final Random random;
    private DeliveryAddress[] deliveryAddresses;

    private int getIndex() {
        return random.nextInt(100);
    }

    public static AddressGenerator getInstance() {
        return addressGenerator;
    }


    public AddressGenerator() {
        final String dataFile = "src/main/resources/data/address.json";
        final ObjectMapper objectMapper;
        random = new Random();
        objectMapper = new ObjectMapper();
        try {
            deliveryAddresses = objectMapper.readValue(new File(dataFile), DeliveryAddress[].class);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public DeliveryAddress getNextAddress() {
        return deliveryAddresses[getIndex()];
    }
}
