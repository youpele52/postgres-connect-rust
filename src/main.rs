use read::queries::DatabaseQueriesRead;
use write::queries::DatabaseQueriesWrite;

mod read;
mod write;

#[tokio::main]
async fn main() {
    let read_queries = read::queries::PostgresQueriesRead;
    let write_queries = write::queries::PostgresQueriesWrite;
    // let table = "geo_data";
    ////// WRITE ///////
    // let _ = write_queries.drop("geo_data").await;
    // let _ = write_queries.drop("sadc_sei").await;
    // let _ = write_queries.split_geojson("/Users/youpele/DevWorld/FZJ/renewableenergydashboard/frontend/src/data/geojson/S2_Expansive_IA1000_OA800.geojson", "/Users/youpele/DevWorld/FZJ/scripts/postgres-connect-rust/files", 1_000_000).await;
    // let _ = write_queries.insert_geojson("/Users/youpele/DevWorld/FZJ/renewableenergydashboard/frontend/src/data/geojson/potential_S2_Expansive_IA800_OA600.geojson",  "geo_data").await;
    // let _ = write_queries.insert_geojson("/Users/youpele/DevWorld/FZJ/renewableenergydashboard/frontend/src/data/geojson",  "geo_data").await;
    // write_queries.fix_collation_version("postgres_db").await;
    // let _ = write_queries.drop_all_tables().await;
    // let _ = write_queries
    //     .backup_database("/Users/youpele/DevWorld/FZJ/db_backups/h2atlas")
    //     .await;
    // let _ = write_queries
    //     .restore_database("/Users/youpele/DevWorld/FZJ/db_backups/h2atlas/backup_h2atlas_2025-03-25_14-57-49.dump", Some("ecowas_gui_db"), Some(16))
    //     .await;

    let _ = write_queries
        .insert_geojson(
            "/Users/youpele/DevWorld/FZJ/data/elyas-gui/nuts3_2024_regions_eez_w_eez.geojson",
            None,
        )
        .await;
    // let _ = write_queries.drop(table).await;
    ////// READ ///////
    let _ = read_queries.list_tables(Some(true)).await;
    // let _ = read_queries.check_postgis_support().await;
    // let _ = read_queries.list_columns(table).await;
    // let _ = read_queries.table_row_count(table).await;
}
