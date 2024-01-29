use serde_json::{json};
use qdrant::*;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let client = qdrant::Qdrant::new();
    // Create a collection with 10-dimensional vectors
    let r = client.create_collection("my_test", 4).await;
    println!("Create collection result is {:?}", r);

    let mut points = Vec::<Point>::new();
    points.push(Point{
        id: PointId::Num(1), vector: vec!(0.05, 0.61, 0.76, 0.74), payload: json!({"city": "Berlin"}).as_object().map(|m| m.to_owned())
    });
    points.push(Point{
        id: PointId::Num(2), vector: vec!(0.19, 0.81, 0.75, 0.11), payload: json!({"city": "London"}).as_object().map(|m| m.to_owned())
    });
    points.push(Point{
        id: PointId::Num(3), vector: vec!(0.36, 0.55, 0.47, 0.94), payload: json!({"city": "Moscow"}).as_object().map(|m| m.to_owned())
    });
    points.push(Point{
        id: PointId::Num(4), vector: vec!(0.18, 0.01, 0.85, 0.80), payload: json!({"city": "New York"}).as_object().map(|m| m.to_owned())
    });
    points.push(Point{
        id: PointId::Num(5), vector: vec!(0.24, 0.18, 0.22, 0.44), payload: json!({"city": "Beijing"}).as_object().map(|m| m.to_owned())
    });
    points.push(Point{
        id: PointId::Num(6), vector: vec!(0.35, 0.08, 0.11, 0.44), payload: json!({"city": "Mumbai"}).as_object().map(|m| m.to_owned())
    });

    let r = client.upsert_points("my_test", points).await;
    println!("Upsert points result is {:?}", r);

    println!("The collection size is {}", client.collection_info("my_test").await);

    let p = client.get_point("my_test", 2).await;
    println!("The second point is {:?}", p);

    let ps = client.get_points("my_test", vec!(1, 2, 3, 4, 5, 6)).await;
    println!("The 1-6 points are {:?}", ps);

    let q = vec![0.2, 0.1, 0.9, 0.7];
    let r = client.search_points("my_test", q, 2).await;
    println!("Search result points are {:?}", r);

    let r = client.delete_points("my_test", vec!(1, 4)).await;
    println!("Delete points result is {:?}", r);

    println!("The collection size is {}", client.collection_info("my_test").await);

    let q = vec![0.2, 0.1, 0.9, 0.7];
    let r = client.search_points("my_test", q, 2).await;
    println!("Search result points are {:?}", r);
    Ok(())
}
