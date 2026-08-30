use futures_util::{StreamExt};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use std::sync::Arc;
use tokio::sync::Mutex;
use crate::gateway::{send_payload};

mod gateway; 


async fn run(token: String) {
    let url = "wss://gateway.discord.gg/?v=10&encoding=json";

    println!("Connecting to Discord Gateway...");

    // (1) Establish connection with gateway. 
    match connect_async(url).await {
        Ok((websocket, response)) => {
            println!("Connected!");
            println!("HTTP Status: {}", response.status()); 
            
            let (mut write, mut read) = websocket.split();
            let (tx, mut rx) = tokio::sync::mpsc::channel(32);

            let seq = Arc::new(Mutex::new(None::<u64>));
            let read_seq = Arc::clone(&seq);
            let write_seq = Arc::clone(&seq);

            // [read task]
            // 서버에서 온 정보를 읽고 데이터를 처리한 후, [write task] 에게 처리한 데이터 전달.
            let read_task = tokio::spawn(async move {
                while let Some(message) = read.next().await {
                    match message {
                        Ok(Message::Text(text)) => {
                            match serde_json::from_str::<gateway::InboundPayload>(&text) {
                                Ok(payload) => {
                                    match payload.op {
                                        gateway::Opcode::Dispatch => {
                                            println!("OPCODE 0 : Recieved Dispatch event"); 

                                                if let Some(s) = payload.s {
                                                    let mut seq = read_seq.lock().await;
                                                    *seq = Some(s);

                                                    println!("Sequence updated: {}", s);
                                                }
                                        }
                                        gateway::Opcode::Hello => {
                                            println!("OPCODE 10 : Recieved Hello event"); 
                                            
                                            let heartbeat_interval = 
                                                payload.d["heartbeat_interval"]
                                                    .as_u64()
                                                    .unwrap(); 
                                            
                                            let heartbeat_tx = tx.clone(); 

                                            tokio::spawn(async move {
                                                let mut interval = tokio::time::interval(
                                                    std::time::Duration::from_millis(heartbeat_interval)
                                                ); 
                                                loop {
                                                    interval.tick().await;

                                                    println!("heartbeat interval"); 

                                                    if heartbeat_tx
                                                        .send(gateway::Opcode::Heartbeat)
                                                        .await
                                                        .is_err() {
                                                            break;
                                                        }
                                                }
                                            }); 

                                            match tx.send(gateway::Opcode::Identify).await {
                                                Ok(()) => (), 
                                                Err(error) => eprintln!("{}", error), 
                                            }
                                        },
                                        _ => println!("{:?}", payload), 
                                    }
                                }, 
                                Err(error) => eprint!("Error: {}", error), 
                            }
                        }, 
                        Ok(message) => println!("recieve: {}", message), 
                        Err(error) => eprintln!("Result Error: {}", error), 
                    }
                }
            }); 
            
            // [write task]
            // [read task] 에서 온 정보를 읽고 데이터를 처리한 후, 서버에게 처리한 데이터 전달.
            let write_task = tokio::spawn(async move {
                while let Some(opcode) = rx.recv().await {
                    match opcode {
                        gateway::Opcode::Heartbeat => {
                            let current_seq = {
                                let seq = write_seq.lock().await;
                                *seq
                            };

                            println!("OPCODE 1 : Sending Heartbeat"); 
                            let payload = gateway::create_heartbeat(current_seq); 
                            send_payload(&mut write, &payload).await.unwrap(); 
                        }
                        gateway::Opcode::Identify => {
                            println!("OPCODE 2 : Sending Identify"); 
                            let payload = gateway::create_identify(&token); 
                            send_payload(&mut write, &payload).await.unwrap();
                        },
                        _ => println!(":("), 
                    }
                }
            });

            let _ = tokio::join!(read_task, write_task);
        }, 
        Err(error) => eprintln!("Gateway Connection Failed: {}", error), 
    }
}


#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();
    let token = std::env::var("TOKEN")
        .expect("TOKEN이 설정되지 않았습니다.");

    run(token.to_string()).await; 
}