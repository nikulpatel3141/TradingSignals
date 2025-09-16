package com.examples.avro;

import example.prices.priceRecord;
import example.prices.prices;
import example.prices.statusEnum;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.File;
import java.io.IOException;

public class Avro {
    public static void main(String[] args) throws IOException {
        writeData();

        readData();
    }

    static void readData() throws IOException {
        DatumReader<prices> userDatumReader = new SpecificDatumReader<>(prices.class);
        File file = new File("scripts/avro/python_price.avro");

        try (DataFileReader<prices> dataFileReader = new DataFileReader<>(file, userDatumReader)) {
            prices price = null;
            while (dataFileReader.hasNext()) {
                price = dataFileReader.next(price);
                System.out.println(price);
            }
        }
    }

    static void writeData() throws IOException {
        prices price = prices.newBuilder()
                .setTicker("javaTicker")
                .setISIN("ABC123")
                .setStatus(statusEnum.OTC)
                .setPrice(new priceRecord(100.45, 200.67))
                .build();

        prices price1 = prices.newBuilder()
                .setTicker("javaTicker1")
                .setISIN(null)
                .setStatus(statusEnum.LISTED)
                .setPrice(new priceRecord(10.45, 20.67))
                .build();

        DatumWriter<prices> userDatumWriter = new SpecificDatumWriter<>(prices.class);
        DataFileWriter<prices> dataFileWriter = new DataFileWriter<>(userDatumWriter);
        dataFileWriter.create(prices.getClassSchema(), new File("scripts/avro/java_price.avro"));
        dataFileWriter.append(price);
        dataFileWriter.append(price1);
        dataFileWriter.close();
    }
}
