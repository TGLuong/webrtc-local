use std::net::IpAddr;

use tokio::sync::mpsc;

use crate::transport::http::HttpOutput;

#[derive(Debug, Clone)]
pub struct HttpContext {
    pub tx: mpsc::Sender<HttpOutput>,
    pub addr: IpAddr,
}

impl HttpContext {
    pub fn new(addr: IpAddr) -> (Self, mpsc::Receiver<HttpOutput>) {
        let (tx, rx) = mpsc::channel(10);
        (Self { tx, addr }, rx)
    }
}
