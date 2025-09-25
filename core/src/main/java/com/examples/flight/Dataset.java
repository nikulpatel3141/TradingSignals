package com.examples.flight;

import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.List;

public record Dataset(List<ArrowRecordBatch> batches, Schema schema, long numRows) implements AutoCloseable {

    @Override
    public void close() throws Exception {
        AutoCloseables.close(batches);
    }
}
