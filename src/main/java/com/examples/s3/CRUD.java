package com.examples.s3;

import org.apache.arrow.dataset.file.FileFormat;
import org.apache.arrow.dataset.file.FileSystemDatasetFactory;
import org.apache.arrow.dataset.jni.NativeDatasetFactory;
import org.apache.arrow.dataset.jni.NativeMemoryPool;
import org.apache.arrow.dataset.source.Dataset;
import org.apache.arrow.dataset.source.DatasetFactory;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.Bucket;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.module.Configuration;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;

public class CRUD {
    static String bucketName = "trading-signals-test-bucket-1";
    static String fileURI = "path/to/iris.parquet";
    static String localFile = "scripts/s3/iris.parquet";

    public static byte[] readLocalFile() throws IOException {
        Path localFile = Paths.get(CRUD.localFile);
        var inputStream = Files.newInputStream(localFile);
        return inputStream.readAllBytes();
    }

    public static Boolean createBucket() throws ExecutionException, InterruptedException {
        boolean bucketExists = false;

        S3AsyncClient client = getS3AsyncClient();
        java.util.List<Bucket> buckets = client.listBuckets().get().buckets();
        for (Bucket bucket : buckets){
            if (Objects.equals(bucket.name(), bucketName)) bucketExists = true;
        }
        if (bucketExists) return false;

        client.createBucket(request -> request.bucket(bucketName)).get();
        return true;
    }

    public static Boolean uploadData() throws IOException, ExecutionException, InterruptedException {
        PutObjectRequest request =  PutObjectRequest
                .builder()
                .bucket(bucketName)
                .key(fileURI)
                .build();

        S3AsyncClient client = getS3AsyncClient();

        var localData = readLocalFile();
        var future = client.putObject(request, AsyncRequestBody.fromBytes(localData));

        future.whenComplete((resp, exception) -> {
            if (exception != null) throw new RuntimeException("Failed to upload file " + localFile);

            System.out.println("Successfully uploaded file " + localFile);
        });
        future.get();
        return true;
    }

    public static byte[] downloadData() throws ExecutionException, InterruptedException {
        var request = GetObjectRequest
                .builder()
                .bucket(bucketName)
                .key(fileURI)
                .build();

        var client = getS3AsyncClient();
        var response = client.getObject(request, AsyncResponseTransformer.toBytes());

        var ref = new Object() {
            byte[] result = null;
        };
        response.whenComplete((objectBytes, exception) -> {
            ref.result = objectBytes.asByteArray();
        });
        response.get();
        return ref.result;
    }

    public static void readParquet(byte[] parquetBytes) throws IOException {
//        List<SimpleGroup> simpleGroups = new ArrayList<>();
//        ParquetStream inputFile = new ParquetStream("my-stream", parquetBytes);
//
//        // Configure Parquet reader
//        Configuration conf = new Configuration();
//        ParquetReader<SimpleGroup> reader = ParquetReader.builder(new GroupReadSupport(), inputFile)
//                .withConf(conf)
//                .build();
//
//        // Read all records
//        SimpleGroup record;
//        while ((record = reader.read()) != null) {
//            records.add(record);
//        }
//
//        reader.close();
        return;
    }

    static S3AsyncClient getS3AsyncClient(){
        return S3AsyncClient.builder().region(Region.EU_NORTH_1).build();
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException, IOException {
        boolean bucketCreated = createBucket();
        System.out.format("Bucket %s %s\n", bucketName, bucketCreated ? "created" : "already exists");

        uploadData();

        var data = downloadData();
        readParquet(data);
    }
}
