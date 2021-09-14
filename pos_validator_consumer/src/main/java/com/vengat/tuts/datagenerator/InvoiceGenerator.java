package com.vengat.tuts.datagenerator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.vengat.tuts.types.DeliveryAddress;
import com.vengat.tuts.types.LineItem;
import com.vengat.tuts.types.PosInvoice;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;


public class InvoiceGenerator {

    private static final Logger logger = LogManager.getLogger();
    private final Random invoiceIndex;
    private final Random invoiceNumber;
    private final Random numberOfItems;
    private PosInvoice[] posInvoices;



    private static InvoiceGenerator invoiceGenerator = new InvoiceGenerator();

    public static InvoiceGenerator getInstance() {
        return invoiceGenerator;
    }

    private InvoiceGenerator() {
        this.invoiceNumber =  new Random();
        invoiceIndex = new Random();
        numberOfItems = new Random();

        String dataFile = "src/main/resources/data/Invoice.json";
        ObjectMapper objectMapper;

        objectMapper = new ObjectMapper();

        try {
            posInvoices = objectMapper.readValue(new File(dataFile), PosInvoice[].class);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private int getIndex() {
        return invoiceIndex.nextInt(100);
    }

    private int getNewInvoiceNumber() {
        return invoiceNumber.nextInt(99999999) + 99999;
    }

    private int getNoOfItems() {
        return numberOfItems.nextInt(4) + 1;
    }

    public PosInvoice getNextInvoice() {
        PosInvoice posInvoice = posInvoices[getIndex()];
        posInvoice.setInvoiceNumber(Integer.toString(getNewInvoiceNumber()));
        posInvoice.setCreatedTime(System.currentTimeMillis());
        if("HOME-DELIVERY".equalsIgnoreCase(posInvoice.getDeliveryType())) {
            DeliveryAddress deliveryAddress = AddressGenerator.getInstance().getNextAddress();
            posInvoice.setDeliveryAddress(deliveryAddress);
        }
        int itemCount = getNoOfItems();
        Double totalAmount = 0.0;
        List<LineItem> lineItems = new ArrayList<>();
        ProductGenerator productGenerator = ProductGenerator.getInstance();
        for(int i = 0; i < itemCount; i++) {
            LineItem lineItem = productGenerator.getNextProduct();
            totalAmount = totalAmount + lineItem.getTotalValue();
            lineItems.add(lineItem);
        }
        posInvoice.setNumberOfItems(itemCount);
        posInvoice.setInvoiceLineItems(lineItems);
        posInvoice.setTotalAmount(totalAmount);
        posInvoice.setTaxableAmount(totalAmount);
        posInvoice.setCGST(totalAmount * 0.025);
        posInvoice.setSGST(totalAmount * 0.025);
        posInvoice.setCESS(totalAmount * 0.00125);
        logger.debug(posInvoice);
        return posInvoice;
    }




}
