package com.poc.parquetdemo.writer;

import com.poc.parquetdemo.dto.User;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.ParquetEncodingException;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;

import java.util.HashMap;
import java.util.List;

public class CustomWriteSupport extends WriteSupport<User> {
    MessageType schema;
    RecordConsumer recordConsumer;
    List<ColumnDescriptor> cols;

    public CustomWriteSupport(MessageType schema) {
        this.schema = schema;
        this.cols = schema.getColumns();
    }

    @Override
    public WriteContext init(Configuration config) {
        return new WriteContext(schema, new HashMap<String, String>());
    }

    @Override
    public void prepareForWrite(RecordConsumer recordConsumer) {
        this.recordConsumer = recordConsumer;
    }

    @Override
    public void write(User user) {

        if (user==null) {
            throw new ParquetEncodingException("Invalid input data. Expecting " +
                    cols.size() + " columns. Input had " + user + " columns (" + cols + ") : " + user);
        }
        recordConsumer.startMessage();
        int col=0;
        writeRecord(col++, String.valueOf(user.getId()));
        writeRecord(col++, String.valueOf(user.getUsername()));
        writeRecord(col, String.valueOf(user.getActive()));
        recordConsumer.endMessage();

    }
    void writeRecord(int col, String val){
        recordConsumer.startField(cols.get(col).getPath()[0], col);
        switch (cols.get(col).getType()) {
                    case BOOLEAN:
                        recordConsumer.addBoolean(Boolean.parseBoolean(val));
                        break;
                    case FLOAT:
                        recordConsumer.addFloat(Float.parseFloat(val));
                        break;
                    case DOUBLE:
                        recordConsumer.addDouble(Double.parseDouble(val));
                        break;
                    case INT32:
                        recordConsumer.addInteger(Integer.parseInt(val));
                        break;
                    case INT64:
                        recordConsumer.addLong(Long.parseLong(val));
                        break;
                    case BINARY:
                        recordConsumer.addBinary(stringToBinary(val));
                        break;
                    default:
                        throw new ParquetEncodingException(
                                "Unsupported column type: " + cols.get(col).getType());
                }
        recordConsumer.endField(cols.get(col).getPath()[0], col);
    }

    private Binary stringToBinary(Object value) {
        return Binary.fromString(value.toString());
    }
}