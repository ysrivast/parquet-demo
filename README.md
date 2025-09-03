# Parquet Demo

This project demonstrates how to read and write Parquet files using Java and Spring Boot. It exposes RESTful endpoints for basic CRUD operations and showcases integration with different formats and cloud storage.

## Features

- Write records to Parquet file (**done**)
- Read records from Parquet file
- Read records by column
- Update records in Parquet file
- Delete records from Parquet file
- Read/Write on the same file
- Support XML format (planned)
- Store file in Amazon S3 (planned)
- Simple UI to consume API (planned)

## Getting Started

### Prerequisites

- Java 11 or above
- Maven

### Build & Run

```bash
mvn clean package
java -jar target/parquet-demo-*.jar
```

### Configuration

Edit `application.properties` to specify file paths, schema locations, and (in future) S3 credentials.

## API Endpoints

| Method | Endpoint         | Description                 |
|--------|------------------|----------------------------|
| GET    | /health          | Health check               |
| POST   | /records         | Write records (XML input)  |
| GET    | /records         | Read all records           |
| PUT    | /records/{id}    | Update record by ID        |
| DELETE | /records/{id}    | Delete record by ID        |

## Usage

- Use tools like Postman or curl to interact with the API.
- Send XML payloads for creating and updating records.

## Development & TODO

- [x] Write records to Parquet
- [ ] Read records from Parquet
- [ ] Read records by column
- [ ] Delete records
- [ ] Update records
- [ ] Support read/write on same file
- [ ] XML format and S3 storage
- [ ] Add web UI

## License



---

Feel free to contribute or open issues for feature requests and improvements.










//    TODO
write--done
read
read based on column
delete
update
r/w on same file

same using xml format

store file with s3


ui to consume it
