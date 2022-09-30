use mysql_wasi::prelude::*;
use mysql_wasi::*;

#[derive(Debug, PartialEq, Eq)]
struct Db {
    host: String,
    db: String,
}

fn main() -> std::result::Result<(), Error> {
    let url = if let Ok(url) = std::env::var("DATABASE_URL") {
        let opts = Opts::from_url(&url).expect("DATABASE_URL invalid");
        if opts
            .get_db_name()
            .expect("a database name is required")
            .is_empty()
        {
            panic!("database name is empty");
        }
        url
    } else {
        "mysql://root:password@127.0.0.1:3306/mysql".to_string()
    };
    let pool = Pool::new(url.as_str())?;
    let mut conn = pool.get_conn()?;

    let selected_dbs = conn.query_map("SELECT Host, Db from db;", |(host, db)| Db { host, db })?;
    dbg!(selected_dbs);
    Ok(())
}
