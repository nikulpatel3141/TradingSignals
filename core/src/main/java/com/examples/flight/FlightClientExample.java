package com.examples.flight;

import org.apache.arrow.flight.Criteria;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

public class FlightClientExample {
    static void main(String[] args){
        Location location = Location.forGrpcInsecure("0.0.0.0", 33333);
        try (
                BufferAllocator allocator = new RootAllocator();
                FlightClient flightClient = FlightClient.builder(allocator, location).build();
        ) {
            var flights = flightClient.listFlights(Criteria.ALL);
            flights.forEach((flight) -> { System.out.println(flight.toString()); });
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
