package com.poc.parquetdemo.config;

import com.poc.parquetdemo.reader.CustomParquetReader;
import com.poc.parquetdemo.writer.CustomParquetWriter;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

@Configuration
public class ParquetConfig {
    @Value("${schema.filePath}")
    private String schemaFilePath;

    @Value("${output.directoryPath}")
    private String outputDirectoryPath;

    @Bean
    public CustomParquetWriter customParquetWriter() throws IOException {
        String outputFilePath = outputDirectoryPath + "/" + System.currentTimeMillis() + ".parquet";
        File outputParquetFile = new File(outputFilePath);
        Path path = new Path(outputParquetFile.toURI().toString());
        System.err.println("path :- " +path);
        return new CustomParquetWriter(
                path, getSchemaForParquetFile(), false, CompressionCodecName.SNAPPY
        );
    }

    private MessageType getSchemaForParquetFile() throws IOException {
        File resource = new File("C:\\workspace\\parquet-demo\\src\\main\\resources\\schemas\\user.schema");
        System.err.println("error1234 :- " +resource.toPath());
        String rawSchema = new String(Files.readAllBytes(resource.toPath()));
        return MessageTypeParser.parseMessageType(rawSchema);
    }

    @Bean
    public CustomParquetReader customParquetReader() throws IOException {
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        String outputFilePath = "C:\\workspace\\parquet-demo\\output\\1712816142131.parquet";
        File outputParquetFile = new File(outputFilePath);
        Path path = new Path(outputParquetFile.toURI().toString());
        ParquetMetadata readFooter = ParquetFileReader.readFooter(conf, path, ParquetMetadataConverter.NO_FILTER);
        MessageType schema = getSchemaForParquetFile();
        return new CustomParquetReader(conf, path,readFooter.getBlocks(), schema);
    }
}
