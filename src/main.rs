use read::queries::DatabaseQueries;

mod read;

#[tokio::main]
async fn main() {
    let queries = read::queries::PostgresQueries;

    // Execute the database query
    // let _ = read::queries::list_columns("geo_data").await;
    let _ = queries.list_tables().await;
    // let _ = queries.list_columns("geo_data").await;
    // match read::queries::main("geo_data").await {
    //     Ok(_) => println!("Query executed successfully"),
    //     Err(e) => eprintln!("Error executing query: {}", e)
    // }
}
