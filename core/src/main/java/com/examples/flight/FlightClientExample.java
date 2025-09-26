package com.examples.flight;

import org.apache.arrow.flight.Criteria;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.Field;

public class FlightClientExample {
    static void main(String[] args) throws Exception {
        Location location = Location.forGrpcInsecure("0.0.0.0", 33333);
        try (
                BufferAllocator allocator = new RootAllocator();
                FlightClient flightClient = FlightClient.builder(allocator, location).build();
        ) {
            var flights = flightClient.listFlights(Criteria.ALL);
            flights.forEach((flight) -> { System.out.println(flight.toString()); });

            var flightInfo = flightClient.getInfo(FlightDescriptor.path("sampleData"));
            System.out.println("Query for sampleData");

            try (var dataStream = flightClient.getStream(flightInfo.getEndpoints().getFirst().getTicket())){
                try (var root = dataStream.getRoot()){
                    var batch = 0;
                    while (dataStream.next()){
                        ++batch;
                        System.out.println("Got batch " + batch);
                        System.out.println(root.contentToTSVString());
                    }
                }
            }


        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
