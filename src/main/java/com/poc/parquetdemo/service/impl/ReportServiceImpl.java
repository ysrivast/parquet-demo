package com.poc.parquetdemo.service.impl;

import com.poc.parquetdemo.dto.User;
import com.poc.parquetdemo.reader.CustomParquetReader;
import com.poc.parquetdemo.service.ReportService;
import com.poc.parquetdemo.writer.CustomParquetWriter;
import lombok.RequiredArgsConstructor;
import lombok.extern.java.Log;
import org.springframework.stereotype.Service;

import java.io.IOException;

@Service
@RequiredArgsConstructor
@Log
public class ReportServiceImpl implements ReportService {


    private final CustomParquetWriter customParquetWriter;

    private  final CustomParquetReader customParquetReader;

    @Override
    public Object getResult(long page, long limit) {
        return null;
    }

    @Override
    public Object create(User user) {
        try {
            customParquetWriter.write(user);
        } catch (IOException e) {
            log.severe("error occured : " +e.getMessage());
            log.severe(e.toString());
        }
        return user;
    }

    @Override
    public Object update(long id, Object o) {
        return null;
    }

    @Override
    public Object delete(long id) {
        return null;
    }

    @Override
    public Object record() {
        customParquetReader.read();
        return "OK";
    }
}
