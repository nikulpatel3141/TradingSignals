package com.examples.flight;

import org.apache.arrow.flight.*;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;


final class SampleData {
    public static Schema getSchema(){
        return new Schema(Arrays.asList(
                new Field("name", FieldType.notNullable(new ArrowType.Utf8()), null),
                new Field("age", FieldType.notNullable(new ArrowType.Int(32, true)), null)
        ));
    }

    public static List<ArrowRecordBatch> getBatches(){
        List<ArrowRecordBatch> batches = new ArrayList<>();
        try (
            BufferAllocator allocator = new RootAllocator();
            var schemaRoot = VectorSchemaRoot.create(getSchema(), allocator);
            var nameVector = (VarCharVector) schemaRoot.getVector("name");
            var ageVector = (IntVector) schemaRoot.getVector("age");
        ){
            ageVector.allocateNew(2);
            nameVector.allocateNew(2);

            ageVector.set(0, 58);
            nameVector.set(0, "Jay".getBytes(StandardCharsets.UTF_8));

            ageVector.set(1, 28);
            nameVector.set(0, "Reian".getBytes(StandardCharsets.UTF_8));

            schemaRoot.setRowCount(2);

            var unloader = new VectorUnloader(schemaRoot);
            try (var batch = unloader.getRecordBatch()) {
                batches.add(batch);
            }

        }
        return batches;
    }

    public static Dataset getDataset(){
        var schema = getSchema();
        var batches = getBatches();
        return new Dataset(batches, schema, 2);
    }
}

public class FlightServerExample extends NoOpFlightProducer implements AutoCloseable {
    BufferAllocator allocator;
    Location location;
    ConcurrentMap<FlightDescriptor, Dataset> datasets;

    public FlightServerExample(BufferAllocator allocator, Location location, ConcurrentMap<FlightDescriptor, Dataset> datasets){
        this.allocator = allocator;
        this.location = location;
        this.datasets = datasets;
    }

    static void main(String[] args){
        var allocator = new RootAllocator();
        var location = Location.forGrpcInsecure("0.0.0.0", 33333);

        var datasets = new ConcurrentHashMap<FlightDescriptor, Dataset>();

        var sampleData = SampleData.getDataset();
        datasets.put(FlightDescriptor.path("sampleData"), sampleData);

        var serverBuilder = FlightServer.builder(allocator, location, new FlightServerExample(allocator, location, datasets));

        try (var server = serverBuilder.build()) {
            server.start();
            System.out.println("Flight server listening on port " + server.getPort());
            server.awaitTermination();
        } catch (InterruptedException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void listFlights(CallContext context, Criteria criteria, StreamListener<FlightInfo> listener){
        datasets.keySet().forEach(((flightDescriptor) -> listener.onNext(getFlightInfo(null, flightDescriptor))));
        listener.onCompleted();
    }

    @Override
    public FlightInfo getFlightInfo(CallContext context, FlightDescriptor descriptor) {
        FlightEndpoint flightEndpoint = new FlightEndpoint(
            new Ticket(descriptor.getPath().getFirst().getBytes(StandardCharsets.UTF_8)),
            location
        );
        return new FlightInfo(
                datasets.get(descriptor).schema(),
                descriptor,
                Collections.singletonList(flightEndpoint),
                /*bytes=*/-1,
                datasets.get(descriptor).numRows()
        );
    }

    @Override
    public void close() throws Exception {
        AutoCloseables.close(datasets.values());
    }
}
