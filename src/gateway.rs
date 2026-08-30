use futures_util::{SinkExt, StreamExt, stream::{SplitSink, SplitStream}};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::MaybeTlsStream;
use tokio::net::TcpStream;
use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};


pub struct Gateway {
    token: String, 
    url: String, 
    session_id: Option<String>, 
    sequence_num: Option<u64>, 
}

impl Gateway {
    fn new(token: String) -> Gateway {
        Gateway { 
            token, 
            url: "wss://gateway.discord.gg/?v=10&encoding=json".to_string(), 
            session_id: None, 
            sequence_num: None, 
        }
    }

    pub async fn send_payload<T: Serialize>(
        write: &mut SplitSink<
            WebSocketStream<MaybeTlsStream<TcpStream>>,
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

#[derive(Debug, Clone, Copy, PartialEq, Serialize_repr, Deserialize_repr)]
#[repr(u8)] 
pub enum Opcode {
    Dispatch = 0,
    Heartbeat = 1,
    Identify = 2,
    PresenceUpdate = 3,
    VoiceStateUpdate = 4,
    Resume = 6,
    Reconnect = 7,
    RequestGuildMembers = 8,
    InvalidSession = 9,
    Hello = 10,
    HeartbeatAck = 11,
    RequestSoundboardSounds = 31,
}


// user -> discord
#[derive(Debug, Serialize)]
pub struct OutboundPayload<T> {
    op: Opcode, 
    d: T,
}

// discord -> user
#[derive(Debug, Deserialize)]
pub struct InboundPayload {
    pub op: Opcode, 
    pub d: serde_json::Value,
    pub s: Option<u64>, 
    pub t: Option<String>, 
}

// OPCOCDE 2 :: Identify
#[derive(Debug, Serialize)]
pub struct IdentifyData<'a> {
    token: &'a str,
    intents: u64,
    properties: IdentifyProperties<'a>,
}

#[derive(Debug, Serialize)]
pub struct IdentifyProperties<'a>{
    os: &'a str,
    browser: &'a str,
    device: &'a str,
}

pub async fn send_payload<T: Serialize>(
    write: &mut SplitSink<
        WebSocketStream<MaybeTlsStream<TcpStream>>,
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

pub fn create_identify(token: &str) -> OutboundPayload<IdentifyData<'_>> {
    OutboundPayload {
        op: Opcode::Identify, 
        d: IdentifyData {
            token: token, 
            intents: 513, 
            properties: IdentifyProperties {
                os: "linux",
                browser: "are-you-titti",
                device: "are-you-titti",
            },
        }
    }
}

pub fn create_heartbeat(seq: Option<u64>) -> OutboundPayload<Option<u64>> {
    OutboundPayload {
        op: Opcode::Heartbeat,
        d: seq,
    }
}