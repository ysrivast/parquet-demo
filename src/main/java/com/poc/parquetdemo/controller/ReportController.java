package com.poc.parquetdemo.controller;


import com.poc.parquetdemo.service.ReportService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/v1/report")
@RequiredArgsConstructor
public class ReportController {

    private final ReportService reportService;

    @GetMapping( value = "/health", produces = MediaType.APPLICATION_XML_VALUE)
    public ResponseEntity<String> health(){
        return ResponseEntity.ok().body("<Health>" +
                "OK" +
                "</Health>");
    }

    @GetMapping( produces = MediaType.APPLICATION_XML_VALUE)
    public ResponseEntity<?> report(
            @RequestParam("limit") long limit,
            @RequestParam("page") long page){
        Object result = reportService.getResult(page, limit);
        return ResponseEntity.ok().body(result);
    }

    @PostMapping( produces = MediaType.APPLICATION_XML_VALUE)
    public ResponseEntity<?> create(
            @RequestBody Object o){
        Object result = reportService.create(o);
        return ResponseEntity.ok().body(result);
    }

    @PutMapping( value = "{id}", produces = MediaType.APPLICATION_XML_VALUE)
    public ResponseEntity<?> update(
            @PathVariable("id") long id,
            @RequestBody Object o){
        Object result = reportService.update(id, o);
        return ResponseEntity.ok().body(result);
    }

    @DeleteMapping( value = "{id}", produces = MediaType.APPLICATION_XML_VALUE)
    public ResponseEntity<?> delete(
            @PathVariable("id") long id){
        Object result = reportService.delete(id);
        return ResponseEntity.ok().body(result);
    }
}
