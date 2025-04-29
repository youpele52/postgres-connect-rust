# postgres-connect-rust

A Rust library and CLI for efficiently uploading GeoJSON data into a PostgreSQL/PostGIS database, with robust table management, error handling, and streaming support.

---

## 🚀 Features

- **Upload GeoJSON to PostgreSQL**: Stream features from large GeoJSON files directly into a database table using the efficient `COPY` command.
- **Automatic Table Creation**: Creates tables if they do not exist, with support for PostGIS geometry columns.
- **Flexible Table Naming**: Use a custom table name or derive it from the file name.
- **Robust Error Handling**: Meaningful error messages and fallbacks for missing or invalid data.
- **Diagnostics**: Informative progress and diagnostic output for all major operations.
- **Extensible**: Modular design for easy integration and testing.

---

## 🛠️ Getting Started

### Prerequisites

- Rust (edition 2021 or later)
- PostgreSQL with [PostGIS](https://postgis.net/) extension enabled

### Installation

Clone the repository:

```bash

git clone https://github.com/youpele52/postgres-connect-rust.git

# or

git clone https://jugit.fz-juelich.de/MichaelYoupele/postgres-connect-rust.git


cd postgres-connect-rust
```

Build the project:

```bash
cargo build --release
```

---

## ⚡ Usage

### Example: Upload a GeoJSON file

```rust
use postgres_connect_rust::write::queries::PostgresQueriesWrite;

#[tokio::main]
async fn main() {
    let write_queries = PostgresQueriesWrite;
    write_queries
        .upload_geojson("path/to/your.geojson", None)
        .await
        .expect("Failed to upload GeoJSON");
}
```

- If you pass `None` as the table name, the table will be named after the file (without extension).

### CLI (if implemented)

```bash
cargo run --release -- upload path/to/your.geojson [table_name]
```

---

## 📚 API Overview

- `upload_geojson(geojson_path: &str, table_name: Option<&str>)`: Uploads a GeoJSON file to the database, creating the table if necessary.
- `process_file(client, input_file, table_name)`: Internal function to stream features into the database.
- Table and column listing, row counting, and PostGIS support checks available in the `read` module.

---

## 📝 Example Output

```text
🔄 Attempting to process file: data/nuts3_2024_regions_eez_w_eez.geojson, table: nuts3_2024_regions_eez_w_eez
🔄 Processing features in data/nuts3_2024_regions_eez_w_eez.geojson
⏳ Closing copy operation...
✅ Copy operation completed successfully!!
```

---

## 🧪 Testing

To run tests:

```bash
cargo test
```

---

## 🗂️ Project Structure

- `src/write/`: Functions for uploading and managing GeoJSON data in PostgreSQL.
- `src/read/`: Functions for querying tables, columns, and row counts.
- `src/main.rs`: Example and CLI entry point.

---

## 🙏 Contributing

Pull requests are welcome! For major changes, please open an issue first to discuss your ideas.

---

## 📄 License

[MIT](LICENSE)

---

## 📞 Contact

For questions or support, please open an issue on GitHub.
