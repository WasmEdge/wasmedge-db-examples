use tokio_postgres::{NoTls, Error};

fn get_url() -> String {
    if let Ok(url) = std::env::var("DATABASE_URL") {
        url
    } else {
        "postgres://postgres@localhost/postgres".into()
    }
}

#[derive(Debug)]
struct Order {
    order_id: i32,
    production_id: i32,
    quantity: i32,
    amount: f32,
    shipping: f32,
    tax: f32,
    shipping_address: String,
}

impl Order {
    fn new(
        order_id: i32,
        production_id: i32,
        quantity: i32,
        amount: f32,
        shipping: f32,
        tax: f32,
        shipping_address: String,
    ) -> Self {
        Self {
            order_id,
            production_id,
            quantity,
            amount,
            shipping,
            tax,
            shipping_address,
        }
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Error> {
    // Connect to the database.
    let (client, connection) = tokio_postgres::connect(&*get_url(), NoTls).await?;
    eprintln!("1");

    // The connection object performs the actual communication with the database,
    // so spawn it off to run on its own.
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });
    eprintln!("2");

    /*
    client.execute("CREATE TABLE orders (order_id INT, production_id INT, quantity INT, amount FLOAT, shipping FLOAT, tax FLOAT, shipping_address VARCHAR(256));", &[]).await?;
    eprintln!("3");

    let orders = vec![
        Order::new(1, 12, 2, 56.0, 15.0, 2.0, String::from("Mataderos 2312")),
        Order::new(2, 15, 3, 256.0, 30.0, 16.0, String::from("1234 NW Bobcat")),
        Order::new(3, 11, 5, 536.0, 50.0, 24.0, String::from("20 Havelock")),
        Order::new(4, 8, 8, 126.0, 20.0, 12.0, String::from("224 Pandan Loop")),
        Order::new(5, 24, 1, 46.0, 10.0, 2.0, String::from("No.10 Jalan Besar")),
    ];
    eprintln!("4");

    for order in orders.iter() {
        client.execute(
            "INSERT INTO orders (order_id, production_id, quantity, amount, shipping, tax, shipping_address) VALUES ($1, $2, $3, $4, $5, $6, $7)",
            &[&order.order_id, &order.production_id, &order.quantity, &order.amount, &order.shipping, &order.tax, &order.shipping_address]
        ).await?;
        eprintln!("5x");
    }

    let rows = client.query("SELECT * FROM orders;", &[]).await?;
    println!("{:#?}", rows);
    */

    // Now we can execute a simple statement that just returns its parameter.
    let rows = client
        .query("SELECT $1::TEXT", &[&"hello WasmEdge"])
        .await?;

    // And then check that we got back the same string we sent over.
    let value: &str = rows[0].get(0);
    println!("{}", value);
    // assert_eq!(value, "hello world");

    Ok(())
}
