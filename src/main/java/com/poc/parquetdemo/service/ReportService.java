package com.poc.parquetdemo.service;

import com.poc.parquetdemo.dto.User;

public interface ReportService {
    Object getResult(long page, long limit);

    Object create(User user);

    Object update(long id, Object o);

    Object delete(long id);

    Object record();
}
