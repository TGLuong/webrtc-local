use std::net::{IpAddr, SocketAddr};

use clap::Parser;
use futures::StreamExt;
use time::macros::format_description;
use tracing_subscriber::{EnvFilter, fmt::time::LocalTime};

use crate::system::System;

pub mod camera;
pub mod session;
pub mod system;
pub mod transport;

#[derive(Debug, Parser)]
pub struct Args {
    #[arg(env, long, default_value = "127.0.0.1:8080")]
    http_addr: SocketAddr,
    #[arg(env, long, default_value = "127.0.0.1")]
    udp_addr: IpAddr,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_timer(LocalTime::new(&format_description!(
            "[year]-[month]-[day] [hour]:[minute]:[second].[subsecond digits:6]"
        )))
        .with_env_filter(EnvFilter::builder().with_default_directive(tracing::Level::INFO.into()).from_env_lossy())
        .init();
    let args = Args::parse();
    log::info!("[main] args: {args:?}");
    let mut system = System::new(args).await?;
    while let Some(()) = system.next().await {}
    log::info!("[main] exit");
    Ok(())
}
