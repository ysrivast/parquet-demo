package com.poc.parquetdemo.reader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

import java.io.IOException;
import java.util.List;

public class CustomParquetReader extends ParquetFileReader {

    MessageType schema;
    List<ColumnDescriptor> cols;


    public CustomParquetReader(Configuration conf, Path path, List<BlockMetaData> readFooter, MessageType schema) throws IOException {
        super(conf, path, readFooter, schema.getColumns());
        this.schema = schema;
        this.cols = schema.getColumns();
    }

    private static void printGroup(Group g) {
        int fieldCount = g.getType().getFieldCount();
        for (int field = 0; field < fieldCount; field++) {
            int valueCount = g.getFieldRepetitionCount(field);

            Type fieldType = g.getType().getType(field);
            String fieldName = fieldType.getName();

            for (int index = 0; index < valueCount; index++) {
                if (fieldType.isPrimitive()) {
                    System.out.println(fieldName + " " + g.getValueToString(field, index));
                }
            }
        }
        System.out.println("");
    }
    public void read(){

        PageReadStore pages = null;
        try {
            while (null != (pages = this.readNextRowGroup())) {
                final long rows = pages.getRowCount();
                System.out.println("Number of rows: " + rows);

                final MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
                final RecordReader recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema));
                for (int i = 0; i < rows; i++) {
                    final Group g = (Group) recordReader.read();
                    printGroup(g);
                }
            }
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        } finally {
            try {
                this.close();
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }

    }
}
