package com.poc.parquetdemo.reader;

import com.poc.parquetdemo.dto.User;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;

import java.util.List;
import java.util.Map;

public class CustomReadSupport extends ReadSupport<User> {

    MessageType schema;
    RecordConsumer recordConsumer;
    List<ColumnDescriptor> cols;
    private final ReadSupport<User> wrapped;

    public CustomReadSupport(MessageType schema, ReadSupport<User> readSupport) {
        this.schema = schema;
        this.cols = schema.getColumns();
        this.wrapped = readSupport;
    }

    @Override
    public ReadContext init(InitContext context) {
        return super.init(context);
    }

    @Override
    public RecordMaterializer<User> prepareForRead(Configuration configuration, Map<String, String> fileMetadata, MessageType messageType, ReadContext readContext) {
        return wrapped.prepareForRead(configuration, fileMetadata, messageType, readContext);
    }


}
