use std::collections::VecDeque;
use std::fmt::Debug;
use std::task::Waker;
use std::{sync::Arc, task::Poll};

use futures::{FutureExt, Stream, StreamExt};
use retina::client::{PacketItem, Playing};
use retina::{
    client::{Credentials, PlayOptions, Session, SessionGroup, SessionOptions, SetupOptions, TeardownPolicy, Transport, UdpTransportOptions},
    codec::FrameFormat,
};
use rtc::rtp::codec::h264::{H264Packet, H264Payloader};
use rtc::rtp::packetizer::{Depacketizer, Packetizer, new_packetizer};
use rtc::rtp::sequence::new_random_sequencer;
use tokio::sync::{mpsc, oneshot};
use url::Url;
use uuid::Uuid;

use crate::transport::rtp::RtpPacket;

#[derive(Debug)]
enum ControlSession {
    Start,
}

pub struct RtspSession {
    id: Uuid,
    tx: mpsc::Sender<ControlSession>,
    rx: Option<oneshot::Receiver<Session<Playing>>>,
    playing: Option<Session<Playing>>,
    outputs: VecDeque<RtpPacket>,
    depacketizer: H264Packet,
    packetizer: Box<dyn Packetizer + Send>,
    waker: Option<Waker>,
}

impl Debug for RtspSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RtspSession")
            .field("id", &self.id)
            .field("outputs", &self.outputs)
            .field("waker", &self.waker)
            .finish()
    }
}

impl RtspSession {
    pub async fn new(id: Uuid, rtsp: String) -> anyhow::Result<Self> {
        let (control_tx, mut control_rx) = mpsc::channel(10);
        let (playing_tx, playing_rx) = oneshot::channel();
        let url = Url::parse(&rtsp)?;
        let user_name = urlencoding::decode(url.username())?.to_string();
        let password = url.password().ok_or(anyhow::anyhow!("missing password"))?;
        let password = urlencoding::decode(password)?.to_string();
        let host = url.host_str().ok_or(anyhow::anyhow!("missing host"))?;
        let port = url.port().ok_or(anyhow::anyhow!("missing port"))?;
        let path = url.path();
        let url = Url::parse(&format!("rtsp://{host}:{port}{path}"))?;
        let creds = Credentials {
            username: user_name,
            password: password,
        };
        let session_group = Arc::new(SessionGroup::default());
        let mut session = Session::describe(
            url,
            SessionOptions::default()
                .creds(Some(creds))
                .session_group(session_group)
                .user_agent("webrtc-local".into())
                .teardown(TeardownPolicy::Always),
        )
        .await?;
        let video_index = session
            .streams()
            .iter()
            .enumerate()
            .find_map(|(i, s)| {
                if s.media() == "video" && s.encoding_name() == "h264" {
                    return Some(i);
                }
                None
            })
            .ok_or(anyhow::anyhow!("not found any video stream"))?;
        session
            .setup(
                video_index,
                SetupOptions::default()
                    .transport(Transport::Udp(UdpTransportOptions::default()))
                    .frame_format(FrameFormat::SIMPLE),
            )
            .await?;
        let depacketizer = H264Packet::default();
        let payloader = H264Payloader::default();
        let packetizer = new_packetizer(1200, 109, rand::random(), Box::new(payloader), Box::new(new_random_sequencer()), 90_000);
        tokio::spawn(async move {
            match control_rx.recv().await {
                Some(cmd) => match cmd {
                    ControlSession::Start => match session.play(PlayOptions::default()).await {
                        Ok(session) => {
                            if let Err(_) = playing_tx.send(session) {
                                log::error!("[RtspSession {id}] playing_tx send error");
                            }
                        }
                        Err(err) => log::error!("[RtspSession {id}] retina play error: {err:?}"),
                    },
                },
                None => log::error!("[RtspSession {id}] control_rx error"),
            }
        });
        Ok(Self {
            id,
            tx: control_tx,
            rx: Some(playing_rx),
            playing: None,
            outputs: VecDeque::new(),
            waker: None,
            depacketizer,
            packetizer: Box::new(packetizer),
        })
    }

    pub fn id(&self) -> Uuid {
        self.id
    }

    pub fn start(&mut self) {
        if let Err(e) = self.tx.try_send(ControlSession::Start) {
            log::error!("[RtspSession {}] send command error: {e:?}", self.id);
        }
    }

    pub fn stop(&mut self) {
        self.playing = None;
    }
}

impl Stream for RtspSession {
    type Item = (Uuid, RtpPacket);

    fn poll_next(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.waker.is_none() {
            this.waker = Some(cx.waker().clone());
        }
        if let Some(rx) = this.rx.as_mut() {
            if let Poll::Ready(Ok(session)) = rx.poll_unpin(cx) {
                this.playing = Some(session);
                this.rx = None;
            }
        }
        if let Some(session) = this.playing.as_mut() {
            while let Poll::Ready(event) = session.poll_next_unpin(cx) {
                match event {
                    Some(event) => match event {
                        Ok(packet) => match packet {
                            PacketItem::Rtp(packet) => {
                                if let Ok(nal) = this.depacketizer.depacketize(&packet.payload().to_vec().into()) {
                                    if let Ok(packets) = this.packetizer.packetize(&nal, 3000) {
                                        for packet in packets.into_iter() {
                                            let rtp = RtpPacket {
                                                sequence: packet.header.sequence_number,
                                                timestamp: packet.header.timestamp,
                                                marker: packet.header.marker,
                                                payload: packet.payload,
                                            };
                                            this.outputs.push_back(rtp);
                                        }
                                    }
                                }
                            }
                            _ => {}
                        },
                        Err(err) => {
                            log::error!("[RtspSession {}] stream error: {err:?}", this.id);
                            return Poll::Ready(None);
                        }
                    },
                    None => return Poll::Ready(None),
                }
            }
            if let Some(out) = this.outputs.pop_front() {
                log::debug!("[RtspSession {}] packet {out:?}", this.id);
                return Poll::Ready(Some((this.id, out)));
            }
        }
        Poll::Pending
    }
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use futures::StreamExt;
    use tokio::time::timeout;
    use uuid::Uuid;

    use crate::rtsp::rtsp_session::RtspSession;

    #[test_log::test(tokio::test)]
    async fn decode_rtsp() -> anyhow::Result<()> {
        let mut session = RtspSession::new(Uuid::now_v7(), "rtsp://admin:LumiVn%402021@10.10.30.86:554/ch1/0".into()).await?;
        let res = timeout(Duration::from_secs(1), session.next()).await;
        log::info!("res: {res:?}");
        session.start();
        for _ in 0..10 {
            let a = session.next().await;
            log::info!("{a:?}");
        }
        Ok(())
    }
}
