package com.poc.parquetdemo;

import com.poc.parquetdemo.dto.User;
import com.poc.parquetdemo.service.ReportService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.List;
import java.util.stream.LongStream;

@SpringBootApplication
public class ParquetDemoApplication implements CommandLineRunner {

	@Autowired
	ReportService reportService;

	public static void main(String[] args) {
		SpringApplication.run(ParquetDemoApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {

		LongStream.range(0,100).mapToObj(e-> new User(e,"Parquet-"+e, e%3==0))
				.forEach(e-> reportService.create(e));
		List<User> users =
				List.of(
						new User(1l,"Alpha", true),
						new User(2l,"Beta", true),
						new User(3l,"Cama", true),
						new User(4l,"Delta", true),
						new User(5l,"Elion", true),
						new User(6l,"Fimor", true),
						new User(7l,"Gouli", true),
						new User(8l,"Hine", true),
						new User(9l,"Ilma", true),
						new User(10l,"Jonh", true),
						new User(11l,"Kool", true),
						new User(12l,"Lul", true),
						new User(13l,"Moop", true),
						new User(14l,"Nol", true)
				);
		users.stream().forEachOrdered(
				e-> reportService.create(e)
		);
	}
}
