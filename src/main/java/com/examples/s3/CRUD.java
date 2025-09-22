package com.examples.s3;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.Bucket;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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
        var bufferAllocator = new RootAllocator();
        var reader = new ArrowStreamReader(new ByteArrayInputStream(parquetBytes), bufferAllocator);

        while (reader.loadNextBatch()) {
            System.out.println(reader.getVectorSchemaRoot().contentToTSVString());
        }
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
