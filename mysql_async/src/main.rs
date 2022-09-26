use mysql_async_wasi::{prelude::*, Opts, Pool, QueryResult, Result};

fn get_url() -> String {
    if let Ok(url) = std::env::var("DATABASE_URL") {
        let opts = Opts::from_url(&url).expect("DATABASE_URL invalid");
        if opts
            .db_name()
            .expect("a database name is required")
            .is_empty()
        {
            panic!("database name is empty");
        }
        url
    } else {
        "mysql://root:pass@127.0.0.1:3306/mysql".into()
    }
}

pub async fn get_all_results<TupleType, P>(
    mut result: QueryResult<'_, '_, P>,
) -> Result<Vec<TupleType>>
where
    TupleType: FromRow + Send + 'static,
    P: Protocol + Send + 'static,
{
    Ok(result.collect().await?)
}

/*
* OrderID integer
* ProductID integer
* Quantity integer
* Amount float
* Shipping float
* Tax float
* ShippingAddress string
*/

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let pool = Pool::new(Opts::from_url(&*get_url()).unwrap());
    let mut conn = pool.get_conn().await.unwrap();
    // create table if no tables exist
    let result = conn
        .query_iter("SHOW TABLES LIKE 'commerce';")
        .await?
        .collect::<String>()
        .await?;
    if result.len() == 0 {
        // table doesn't exist, create a new one
        conn
            .query_iter("CREATE TABLE commerce (OrderID INT, ProductID INT, Quantity INT, Amount FLOAT, Shipping FLOAT, Tax FLOAT, ShippingAddress VARCHAR(20));")
            .await?
            .collect::<String>()
            .await?;
        println!("create new table");
    } else {
        // delete all data from the table.
        println!("delete all from commerce");
        let _ = conn.query_iter("DELETE FROM commerce;").await?;
    }

    // insert some data
    let _ = conn
        .query_iter(
            "INSERT INTO commerce VALUES 
    (1, 12, 2, 56.0, 15.0, 2.0, 'Mataderos 2312'),
    (2, 15, 3, 256.0, 30.0, 16.0, '1234 NW Bobcat Lane'),
    (3, 11, 5, 536.0, 50.0, 24.0, '20 Havelock'),
    (4, 8, 8, 126.0, 20.0, 12.0, '224 Pandan Loop'),
    (5, 24, 1, 46.0, 10.0, 2.0, 'No.10 Jalan Besar');",
        )
        .await?;

    // query data
    let result = conn
        .query_iter("SELECT * from commerce;")
        .await?
        .collect::<(i32, i32, i32, f32, f32, f32, String)>()
        .await?;
    dbg!(result.len());
    dbg!(result);

    // delete some data
    let _ = conn
        .query_iter("DELETE FROM commerce WHERE OrderID=4;")
        .await?;
    // query data
    let result = conn
        .query_iter("SELECT * from commerce;")
        .await?
        .collect::<(i32, i32, i32, f32, f32, f32, String)>()
        .await?;
    dbg!(result.len());
    dbg!(result);

    // update some data
    let _ = conn
        .query_iter(
            "UPDATE commerce
    SET ShippingAddress = '8366 Elizabeth St.'
    WHERE OrderID = 2;",
        )
        .await?;
    // query data
    let result = conn
        .query_iter("SELECT * from commerce;")
        .await?
        .collect::<(i32, i32, i32, f32, f32, f32, String)>()
        .await?;
    dbg!(result.len());
    dbg!(result);

    drop(conn);
    pool.disconnect().await.unwrap();
    Ok(())
}
