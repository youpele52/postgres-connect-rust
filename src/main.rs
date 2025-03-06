use read::queries::DatabaseQueriesRead;
use write::queries::DatabaseQueriesWrite;

mod read;
mod write;

#[tokio::main]
async fn main() {
    let read_queries = read::queries::PostgresQueriesRead;
    let write_queries = write::queries::PostgresQueriesWrite;
    
    ////// WRITE ///////
    let _ = write_queries.drop("geo_data").await;
    
    ////// READ ///////
    // let ggg =
    // let _ = read_queries.list_columns("geo_data").await;
    // let _ = read_queries.list_tables().await;

    // let _ = queries.list_columns("geo_data").await;
    // match read::queries::main("geo_data").await {
    //     Ok(_) => println!("Query executed successfully"),
    //     Err(e) => eprintln!("Error executing query: {}", e)
    // }
}
