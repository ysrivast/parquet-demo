package com.poc.parquetdemo.controller;


import com.poc.parquetdemo.dto.User;
import com.poc.parquetdemo.dto.UserList;
import com.poc.parquetdemo.service.ReportService;
import lombok.RequiredArgsConstructor;
import lombok.extern.java.Log;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import static org.springframework.http.MediaType.APPLICATION_XML_VALUE;

@RestController
@RequestMapping("/v1/report")
@RequiredArgsConstructor
@Log
public class ReportController {

    private final ReportService reportService;

    @GetMapping( value = "/health", produces = APPLICATION_XML_VALUE)
    public ResponseEntity<String> health(){
        return ResponseEntity.ok().body("<Health>" +
                "OK" +
                "</Health>");
    }

    @GetMapping( value = "/record", produces = APPLICATION_XML_VALUE)
    public ResponseEntity<?> record(){

        Object result = reportService.record();
        return ResponseEntity.ok().body(result);
    }

    @GetMapping( produces = APPLICATION_XML_VALUE)
    public ResponseEntity<UserList> report(
            @RequestParam("limit") long limit,
            @RequestParam("page") long page){
        UserList result = reportService.getResult(page, limit);
        return ResponseEntity.ok().body(result);
    }

    @PostMapping( consumes = APPLICATION_XML_VALUE)
    public ResponseEntity<?> create(
            @RequestBody User user){
        log.info("user create request : "+ user);
        Object result = reportService.create(user);
        return ResponseEntity.ok().body(result);
    }

    @PutMapping( value = "{id}", produces = APPLICATION_XML_VALUE)
    public ResponseEntity<?> update(
            @PathVariable("id") long id,
            @RequestBody Object o){
        Object result = reportService.update(id, o);
        return ResponseEntity.ok().body(result);
    }

    @DeleteMapping( value = "{id}", produces = APPLICATION_XML_VALUE)
    public ResponseEntity<?> delete(
            @PathVariable("id") long id){
        Object result = reportService.delete(id);
        return ResponseEntity.ok().body(result);
    }
}
