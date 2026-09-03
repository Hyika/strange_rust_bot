use crate::gateway::{Gateway};

mod gateway; 


async fn run(token: String) {
    let mut gateway = Gateway::new(token); 

    if let Err(e) = gateway.connect().await {
        eprintln!("Error connecting to gateway: {}", e);
    }
}

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();
    let token = std::env::var("TOKEN")
        .expect("TOKEN이 설정되지 않았습니다.");

    run(token.to_string()).await; 
}