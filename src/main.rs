use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::{collections::HashSet, env, net::SocketAddr, sync::Arc};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    sync::RwLock,
};
use tracing::{error, info, warn};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Message {
    Hello { node_id: String, listen_addr: SocketAddr },
    Chat { from: String, text: String },
    PeerList { peers: Vec<SocketAddr> },
    Ping,
    Pong,
}

type Peers = Arc<RwLock<HashSet<SocketAddr>>>;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_target(false)
        .init();

    let args: Vec<String> = env::args().collect();
    if args.len() < 3 {
        eprintln!("Використання: {} <node_id> <listen_addr> [peer_addr]", args[0]);
        eprintln!("Приклад: RUST_LOG=info cargo run -- node-a 127.0.0.1:7001");
        return Ok(());
    }

    let node_id = args[1].clone();
    let listen_addr: SocketAddr = args[2].parse().context("Некоректна адреса вузла")?;
    let bootstrap = args.get(3).map(|s| s.parse()).transpose().context("Некоректна адреса peer")?;
    let peers: Peers = Arc::new(RwLock::new(HashSet::new()));

    let listener_peers = peers.clone();
    let listener_id = node_id.clone();
    tokio::spawn(async move {
        if let Err(e) = listen(listener_id, listen_addr, listener_peers).await {
            error!(error = %e, "Сервер зупинено");
        }
    });

    if let Some(peer) = bootstrap {
        peers.write().await.insert(peer);
        if let Err(e) = introduce(&node_id, listen_addr, peer, &peers).await {
            warn!(peer = %peer, error = %e, "Не вдалося підключитися до початкового вузла");
        }
    }

    command_loop(node_id, listen_addr, peers).await
}

async fn listen(node_id: String, listen_addr: SocketAddr, peers: Peers) -> Result<()> {
    let listener = TcpListener::bind(listen_addr).await?;
    info!(node = %node_id, address = %listen_addr, "Вузол запущено");
    loop {
        let (stream, remote) = listener.accept().await?;
        let id = node_id.clone();
        let peers = peers.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_connection(id, stream, remote, peers).await {
                warn!(remote = %remote, error = %e, "Помилка з'єднання");
            }
        });
    }
}

async fn handle_connection(node_id: String, stream: TcpStream, remote: SocketAddr, peers: Peers) -> Result<()> {
    let (reader, mut writer) = stream.into_split();
    let mut lines = BufReader::new(reader).lines();
    while let Some(line) = lines.next_line().await? {
        let message: Message = serde_json::from_str(&line).context("Некоректне повідомлення протоколу")?;
        info!(remote = %remote, message = ?message, "Отримано повідомлення");
        match message {
            Message::Hello { node_id: peer_id, listen_addr } => {
                peers.write().await.insert(listen_addr);
                info!(peer = %peer_id, address = %listen_addr, "Новий вузол підключено");
                let known: Vec<_> = peers.read().await.iter().copied().collect();
                write_message(&mut writer, &Message::PeerList { peers: known }).await?;
            }
            Message::Chat { from, text } => println!("\n[{from}] {text}"),
            Message::PeerList { peers: discovered } => {
                let mut known = peers.write().await;
                known.extend(discovered);
            }
            Message::Ping => write_message(&mut writer, &Message::Pong).await?,
            Message::Pong => info!(remote = %remote, "Pong отримано"),
        }
    }
    Ok(())
}

async fn introduce(node_id: &str, listen_addr: SocketAddr, peer: SocketAddr, peers: &Peers) -> Result<()> {
    let mut stream = TcpStream::connect(peer).await?;
    write_message(&mut stream, &Message::Hello { node_id: node_id.to_owned(), listen_addr }).await?;
    let mut line = String::new();
    BufReader::new(&mut stream).read_line(&mut line).await?;
    if !line.trim().is_empty() {
        if let Message::PeerList { peers: discovered } = serde_json::from_str(line.trim())? {
            peers.write().await.extend(discovered);
        }
    }
    info!(peer = %peer, "Підключено до вузла");
    Ok(())
}

async fn send_chat(node_id: &str, target: SocketAddr, text: String) -> Result<()> {
    let mut stream = TcpStream::connect(target).await?;
    write_message(&mut stream, &Message::Chat { from: node_id.to_owned(), text }).await
}

async fn write_message<W: AsyncWriteExt + Unpin>(writer: &mut W, message: &Message) -> Result<()> {
    let mut bytes = serde_json::to_vec(message)?;
    bytes.push(b'\n');
    writer.write_all(&bytes).await?;
    writer.flush().await?;
    Ok(())
}

async fn command_loop(node_id: String, listen_addr: SocketAddr, peers: Peers) -> Result<()> {
    println!("Команди: /peers, /connect <addr>, /send <addr> <text>, /broadcast <text>, /quit");
    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    while let Some(line) = lines.next_line().await? {
        let line = line.trim();
        if line == "/quit" { break; }
        if line == "/peers" {
            let list = peers.read().await;
            println!("Відомі вузли: {list:?}");
        } else if let Some(addr) = line.strip_prefix("/connect ") {
            let addr: SocketAddr = addr.parse()?;
            peers.write().await.insert(addr);
            introduce(&node_id, listen_addr, addr, &peers).await?;
        } else if let Some(rest) = line.strip_prefix("/send ") {
            let (addr, text) = rest.split_once(' ').context("Формат: /send <addr> <text>")?;
            send_chat(&node_id, addr.parse()?, text.to_owned()).await?;
        } else if let Some(text) = line.strip_prefix("/broadcast ") {
            let targets: Vec<_> = peers.read().await.iter().copied().collect();
            for target in targets {
                if let Err(e) = send_chat(&node_id, target, text.to_owned()).await {
                    warn!(peer = %target, error = %e, "Не вдалося надіслати повідомлення");
                }
            }
        } else {
            println!("Невідома команда");
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn protocol_round_trip() {
        let msg = Message::Chat { from: "node-a".into(), text: "Привіт".into() };
        let json = serde_json::to_string(&msg).unwrap();
        assert_eq!(serde_json::from_str::<Message>(&json).unwrap(), msg);
    }
}
