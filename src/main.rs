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
    // let _ = write_queries.split_geojson("/Users/youpele/DevWorld/FZJ/renewableenergydashboard/frontend/src/data/geojson/S2_Expansive_IA1000_OA800.geojson", "/Users/youpele/DevWorld/FZJ/scripts/postgres-connect-rust/files", 1_000_000).await;
    // let _ = write_queries.insert_geojson("/Users/youpele/DevWorld/FZJ/renewableenergydashboard/frontend/src/data/geojson/potential_S2_Expansive_IA800_OA600.geojson",  "geo_data").await;
    let _ = write_queries.insert_geojson("/Users/youpele/DevWorld/FZJ/renewableenergydashboard/frontend/src/data/geojson",  "geo_data").await;
    // write_queries.fix_collation_version("postgres_db").await;

    ////// READ ///////
    // let _ = read_queries.check_postgis_support().await;
    // let _ = read_queries.list_columns("geo_data").await;
    // let _ = read_queries.table_row_count("geo_data").await;
    // let _ = read_queries.list_tables().await;

    // let _ = read_queries.list_columns("geo_data").await;
    // match read::queries::main("geo_data").await {
    //     Ok(_) => println!("Query executed successfully"),
    //     Err(e) => eprintln!("Error executing query: {}", e)
    // }
}
