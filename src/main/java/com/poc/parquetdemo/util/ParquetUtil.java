package com.poc.parquetdemo.util;

import com.poc.parquetdemo.dto.User;
import com.poc.parquetdemo.reader.CustomReadSupport;
import com.poc.parquetdemo.writer.CustomWriteSupport;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Map;

public class ParquetUtil {

    public static void writeToParquet(User bean) throws IOException  {
        ParquetWriter writer = getParquetWriter();
        writer.write(bean);
        writer.close();
    }

    public static void readFromParquet( ) throws IOException  {
        ParquetReader reader = getParquetReader();
        User user =(User)  reader.read();
        reader.close();
    }

    private static ParquetReader getParquetReader() throws IOException {
        String outputFilePath =  "output/test.parquet";
        File outputParquetFile = new File(outputFilePath);
        Path path = new Path(outputParquetFile.toURI().toString());
        return new ParquetReader(path, new GroupReadSupport());
    }

    private static ParquetWriter getParquetWriter() throws IOException {
        String outputFilePath =  "output/test.parquet";
        File outputParquetFile = new File(outputFilePath);
        Path path = new Path(outputParquetFile.toURI().toString());
//        ParquetWriter.Builder(path).
        ParquetWriter.Builder parquetWriterbuilder = new ParquetWriter.Builder(path) {
            @Override
            protected ParquetWriter.Builder self() {
                return this;
            }

            @Override
            protected WriteSupport getWriteSupport(Configuration conf) {
                try {
                    return new CustomWriteSupport(getSchemaForParquetFile());
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        };
        parquetWriterbuilder
//                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .withCompressionCodec(CompressionCodecName.SNAPPY);
        System.err.println("path :- " +path);
        return  parquetWriterbuilder.build();
    }
    private static MessageType getSchemaForParquetFile() throws IOException {
        File resource = new File("C:\\workspace\\parquet-demo\\src\\main\\resources\\schemas\\user.schema");
        System.err.println("error1234 :- " +resource.toPath());
        String rawSchema = new String(Files.readAllBytes(resource.toPath()));
        return MessageTypeParser.parseMessageType(rawSchema);
    }

}
