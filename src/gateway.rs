use futures_util::{SinkExt, StreamExt, stream::{SplitSink, SplitStream}};
use tokio_tungstenite::{connect_async, WebSocketStream, tungstenite::protocol::Message};
use tokio::{sync::Mutex, net::TcpStream};
use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};
use std::{f32::consts::E, sync::Arc};


#[derive(Serialize_repr, Debug)]
#[repr(u8)]
pub enum OutboundOpcode {
    Heartbeat = 1,
    Identify = 2,
    PresenceUpdate = 3,
    VoiceStateUpdate = 4,
    Resume = 6,
    RequestGuildMembers = 8,
    RequestSoundboardSounds = 31,
}

// user -> discord
#[derive(Debug, Serialize)]
pub struct OutboundPayload<T> {
    op: OutboundOpcode, 
    d: T,
}

impl<T> OutboundPayload<T> {
    pub fn to_message(&self) -> Result<Message, Box<dyn std::error::Error + Send + Sync>>
    where
        T: Serialize,
    {
        let json = serde_json::to_string(self)?;
        Ok(Message::Text(json.into()))
    }
}

#[derive(Deserialize_repr, Debug)]
#[repr(u8)]
pub enum InboundOpcode {
    Dispatch = 0,
    Reconnect = 7,
    InvalidSession = 9,
    Hello = 10,
    HeartbeatAck = 11,
}

// discord -> user
#[derive(Debug, Deserialize)]
pub struct InboundPayload {
    pub op: InboundOpcode, 
    pub d: serde_json::Value,
    pub s: Option<u64>, 
    pub t: Option<String>, 
}

// OPCOCDE 2 :: Identify
#[derive(Debug, Serialize)]
pub struct IdentifyData {
    token: String,
    intents: u64,
    properties: IdentifyProperties,
}

#[derive(Debug, Serialize)]
pub struct IdentifyProperties{
    os: String,
    browser: String,
    device: String,
}

pub struct Gateway {
    token: String, 
    url: String, 
    session_id: Arc<Mutex<Option<String>>>, 
    sequence_num: Arc<Mutex<Option<u64>>>, 
}

impl Gateway {
    pub fn new(token: String) -> Gateway {
        Gateway {
            token, 
            url: "wss://gateway.discord.gg/?v=10&encoding=json".to_string(), 
            session_id: Arc::new(Mutex::new(None)),
            sequence_num: Arc::new(Mutex::new(None)),
        }
    }

    pub async fn connect(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let token = self.token.clone();

        let (ws_stream, _) = connect_async(&self.url).await?;

        let (mut write, mut read) = ws_stream.split();
        
        let (tx, mut rx) = tokio::sync::mpsc::channel::<OutboundOpcode>(32);

        let write_seq_num = Arc::clone(&self.sequence_num);
        let read_seq_num = Arc::clone(&self.sequence_num);

        let sending_task = tokio::spawn(async move {
            while let Some(op) = rx.recv().await {
                let message = match op {
                    OutboundOpcode::Identify => OutboundPayload {
                            op: OutboundOpcode::Identify,
                            d: IdentifyData {
                                token: token.clone(),
                                intents: 513, // Example intents
                                properties: IdentifyProperties {
                                    os: "linux".to_string(),
                                    browser: "are_you_titti".to_string(),
                                    device: "are_you_titti".to_string(),
                                },
                            },
                        }.to_message(),
                    OutboundOpcode::Heartbeat => OutboundPayload {
                            op: OutboundOpcode::Heartbeat,
                            d: write_seq_num.lock().await.clone(),
                        }.to_message(), 
                    _ => continue, 
                    // OutboundOpcode::PresenceUpdate => {}
                    // OutboundOpcode::VoiceStateUpdate => {}
                    // OutboundOpcode::Resume => {}
                    // OutboundOpcode::RequestGuildMembers => {}
                    // OutboundOpcode::RequestSoundboardSounds => {}
                };
                println!("Sending message: {:?}", message);
                write.send(message?).await?;
            }

            Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
        }); 

        let listening_task = tokio::spawn(async move {
            while let Some(message) = read.next().await {
                match message {
                    Ok(Message::Text(message)) => {
                        match serde_json::from_str::<InboundPayload>(&message) {
                            Ok(payload) => {
                                match payload.op {
                                    InboundOpcode::Dispatch => {
                                        println!("OPCODE 0 : Received Dispatch event");

                                        if let Some(num) = payload.s {
                                            let mut seq = read_seq_num.lock().await;
                                            *seq = Some(num);

                                            println!("Sequence updated: {}", num);
                                        }
                                    }
                                    InboundOpcode::Reconnect => {}
                                    InboundOpcode::InvalidSession => {}
                                    InboundOpcode::Hello => {
                                        println!("OPCODE 10 : Received Hello event");

                                        tx.send(OutboundOpcode::Identify).await?;
                                    
                                        let heartbeat_interval = payload.d["heartbeat_interval"]
                                            .as_u64()
                                            .ok_or("Missing heartbeat_interval in Hello payload")?;

                                        println!("Heartbeat interval: {}", heartbeat_interval);

                                        let heartbeat_tx = tx.clone(); 
                                        
                                        tokio::spawn(async move {
                                            let mut interval = tokio::time::interval(
                                                std::time::Duration::from_millis(heartbeat_interval)
                                            );

                                            loop {
                                                interval.tick().await;

                                                println!("Sending heartbeat");
                                                if let Err(e) = heartbeat_tx.send(OutboundOpcode::Heartbeat).await {
                                                    eprintln!("Failed to send heartbeat to channel: {}", e);
                                                    break; // 채널이 끊겼으므로 하트비트 루프 종료
                                                }
                                            }

                                            Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
                                        });
                                    }
                                    InboundOpcode::HeartbeatAck => {}
                                }
                            }
                            Err(error) => {
                                eprintln!("Error parsing message: {}", error);
                            }
                        }
                    }
                    Ok(message) => {
                        println!("Received non-text message: {:?}", message);
                    }
                    Err(error) => {
                        eprintln!("Error reading message: {}", error);
                        break;
                    }
                }
            }

            Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
        }); 

        let _ = tokio::join!(listening_task, sending_task); 
        Ok(())
    }

    pub async fn send_payload<T: Serialize>(
        write: &mut SplitSink<
            WebSocketStream<tokio_tungstenite::MaybeTlsStream<TcpStream>>,
            Message
        >,
        payload: &OutboundPayload<T>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let json = serde_json::to_string(payload)?;

        write
            .send(Message::Text(json.into()))
            .await?;

        Ok(())
    }
}