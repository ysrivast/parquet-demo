package com.poc.parquetdemo.service;

public interface ReportService {
    Object getResult(long page, long limit);

    Object create(Object o);

    Object update(long id, Object o);

    Object delete(long id);
}
