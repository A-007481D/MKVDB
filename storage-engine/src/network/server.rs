use crate::engine::ApexEngine;
use crate::network::resp::{RespCodec, RespValue};
use futures_util::{SinkExt, StreamExt};
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::Framed;
use crate::error::Result;

pub struct ApexServer {
    engine: Arc<ApexEngine>,
}

impl ApexServer {
    pub fn new(engine: Arc<ApexEngine>) -> Self {
        Self { engine }
    }

    pub async fn run(&self, addr: &str, mut shutdown: tokio::sync::oneshot::Receiver<()>) -> Result<()> {
        let listener = TcpListener::bind(addr).await?;
        tracing::info!("🚀 MKVDB Server listening on {}", addr);

        loop {
            tokio::select! {
                accept_res = listener.accept() => {
                    let (socket, _) = accept_res?;
                    let engine = self.engine.clone();

                    tokio::spawn(async move {
                        if let Err(e) = Self::handle_connection(socket, engine).await {
                            tracing::error!("Connection error: {:?}", e);
                        }
                    });
                }
                _ = &mut shutdown => {
                    tracing::info!("Server received shutdown signal. Stopping listener...");
                    break;
                }
            }
        }

        Ok(())
    }

    async fn handle_connection(socket: TcpStream, engine: Arc<ApexEngine>) -> Result<()> {
        let mut framed = Framed::new(socket, RespCodec);
        let peer_addr = framed.get_ref().peer_addr().ok();
        
        tracing::info!("New connection from {:?}", peer_addr);

        while let Some(request) = framed.next().await {
            // BACKPRESSURE: If the engine flusher is overwhelmed, stop reading from the socket.
            // This forces the client to slow down due to TCP window exhaustion.
            while engine.is_saturated() {
                tracing::warn!("Engine is saturated, applying backpressure to {:?}", peer_addr);
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }

            let request = request?;
            tracing::debug!("Received request: {:?}", request);

            let response = match request {
                RespValue::Array(Some(mut args)) if !args.is_empty() => {
                    Self::dispatch_command(&engine, &mut args).await
                }
                RespValue::SimpleString(s) if s.to_uppercase() == "PING" => {
                    RespValue::SimpleString("PONG".to_string())
                }
                _ => {
                    tracing::warn!("Received invalid request format: {:?}", request);
                    RespValue::Error("ERR unknown command or invalid format".to_string())
                }
            };

            framed.send(response).await?;
        }

        tracing::info!("Connection closed for {:?}", peer_addr);
        Ok(())
    }

    async fn dispatch_command(engine: &Arc<ApexEngine>, args: &mut [RespValue]) -> RespValue {
        let cmd_name = match &args[0] {
            RespValue::BulkString(Some(b)) => String::from_utf8_lossy(b).to_uppercase(),
            _ => return RespValue::Error("ERR invalid command format".to_string()),
        };

        match cmd_name.as_str() {
            "SET" => {
                if args.len() != 3 {
                    return RespValue::Error("ERR wrong number of arguments for 'set' command".to_string());
                }
                let key = match &args[1] {
                    RespValue::BulkString(Some(b)) => b.clone(),
                    _ => return RespValue::Error("ERR invalid key".to_string()),
                };
                let val = match &args[2] {
                    RespValue::BulkString(Some(b)) => b.clone(),
                    _ => return RespValue::Error("ERR invalid value".to_string()),
                };

                match engine.put(key, val).await {
                    Ok(_) => RespValue::SimpleString("OK".to_string()),
                    Err(e) => RespValue::Error(format!("ERR storage error: {:?}", e)),
                }
            }
            "GET" => {
                if args.len() != 2 {
                    return RespValue::Error("ERR wrong number of arguments for 'get' command".to_string());
                }
                let key = match &args[1] {
                    RespValue::BulkString(Some(b)) => b,
                    _ => return RespValue::Error("ERR invalid key".to_string()),
                };

                match engine.get(key) {
                    Ok(Some(val)) => RespValue::BulkString(Some(val)),
                    Ok(None) => RespValue::BulkString(None), // Nil
                    Err(e) => RespValue::Error(format!("ERR storage error: {:?}", e)),
                }
            }
            "DEL" => {
                if args.len() != 2 {
                    return RespValue::Error("ERR wrong number of arguments for 'del' command".to_string());
                }
                let key = match &args[1] {
                    RespValue::BulkString(Some(b)) => b.clone(),
                    _ => return RespValue::Error("ERR invalid key".to_string()),
                };

                match engine.delete(key).await {
                    Ok(_) => RespValue::Integer(1),
                    Err(e) => RespValue::Error(format!("ERR storage error: {:?}", e)),
                }
            }
            "SCAN" => {
                // Usage: SCAN start_key end_key
                if args.len() != 3 {
                    return RespValue::Error("ERR usage: SCAN start_key end_key".to_string());
                }
                let start = match &args[1] {
                    RespValue::BulkString(Some(b)) => b.clone(),
                    _ => return RespValue::Error("ERR invalid start_key".to_string()),
                };
                let end = match &args[2] {
                    RespValue::BulkString(Some(b)) => b.clone(),
                    _ => return RespValue::Error("ERR invalid end_key".to_string()),
                };

                match engine.scan(start, end) {
                    Ok(mut stream) => {
                        let mut results = Vec::new();
                        while let Some(res) = stream.next().await {
                            match res {
                                Ok((k, v)) => {
                                    results.push(RespValue::BulkString(Some(k)));
                                    results.push(RespValue::BulkString(Some(v)));
                                }
                                Err(e) => return RespValue::Error(format!("ERR scan error: {:?}", e)),
                            }
                        }
                        RespValue::Array(Some(results))
                    }
                    Err(e) => RespValue::Error(format!("ERR scan error: {:?}", e)),
                }
            }
            "PING" => RespValue::SimpleString("PONG".to_string()),
            "COMMAND" => RespValue::Array(Some(vec![])),
            "HELLO" => RespValue::Array(Some(vec![])),
            "CLIENT" => RespValue::SimpleString("OK".to_string()),
            _ => RespValue::Error(format!("ERR unknown command '{}'", cmd_name)),
        }
    }
}
