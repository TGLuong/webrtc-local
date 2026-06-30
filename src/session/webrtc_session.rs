use std::{collections::VecDeque, net::SocketAddr, pin::Pin, task::Poll, time::Instant};

use futures::{FutureExt, Stream};
use str0m::{
    Input, Output, Rtc,
    media::{Mid, Pt},
    net::{Protocol, Receive},
    rtp::{RtpWrite, SeqNo},
};
use tokio::{
    io::ReadBuf,
    net::UdpSocket,
    time::{Sleep, sleep_until},
};
use uuid::Uuid;

use crate::transport::rtp::RtpPacket;

#[derive(Debug)]
pub struct WebrtcSession {
    id: Uuid,
    rtc: Rtc,
    udp: UdpSocket,
    destination: SocketAddr,
    out_queue: VecDeque<(SocketAddr, Vec<u8>)>,
    timeout: Option<Pin<Box<Sleep>>>,
    connected: bool,
    video_mid: Option<Mid>,
    video_pt: Option<Pt>,
    seq_no: SeqNo,
}

impl WebrtcSession {
    pub fn new(rtc: Rtc, udp: UdpSocket, destination: SocketAddr, video_pt: Option<Pt>) -> Self {
        let id = Uuid::now_v7();
        let out_queue = VecDeque::new();
        Self {
            id,
            rtc,
            udp,
            destination,
            out_queue,
            timeout: None,
            connected: false,
            video_mid: None,
            video_pt,
            seq_no: SeqNo::from(0),
        }
    }

    pub fn id(&self) -> Uuid {
        self.id
    }

    pub fn send_video(&mut self, rtp: RtpPacket) -> anyhow::Result<()> {
        if let (Some(mid), Some(pt)) = (self.video_mid, self.video_pt) {
            let mut api = self.rtc.direct_api();
            if let Some(stream) = api.stream_tx_by_mid(mid, None) {
                let rtp_write = RtpWrite::new(pt, self.seq_no, rtp.timestamp, Instant::now(), rtp.payload.to_vec()).marker(rtp.marker);
                self.seq_no.inc();
                stream.write_rtp(rtp_write);
            }
        }
        Ok(())
    }
}

impl Stream for WebrtcSession {
    type Item = ();

    fn poll_next(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Option<Self::Item>> {
        let this = self.get_mut();

        let mut storage = [0u8; 2048];
        let mut read_buf = ReadBuf::new(&mut storage);
        while let Poll::Ready(event) = this.udp.poll_recv_from(cx, &mut read_buf) {
            match event {
                Ok(addr) => match Receive::new(Protocol::Udp, addr, this.destination, read_buf.filled()) {
                    Ok(input) => {
                        if let Err(e) = this.rtc.handle_input(Input::Receive(Instant::now(), input)) {
                            log::error!("[WebrtcSession {}] handle_input error: {e:?}", this.id);
                        }
                    }
                    Err(err) => {
                        log::error!(
                            "[WebrtcSession {}] invalid udp packet from {addr}, len={}: {err:?}",
                            this.id,
                            read_buf.filled().len()
                        );
                    }
                },
                Err(err) => {
                    log::error!("[WebrtcSession {}] udp error: {err}", this.id);
                    return Poll::Ready(None);
                }
            }
            read_buf.clear();
        }
        loop {
            match this.rtc.poll_output() {
                Ok(out) => match out {
                    Output::Timeout(instant) => {
                        this.timeout = Some(Box::pin(sleep_until(instant.into())));
                        break;
                    }
                    Output::Transmit(transmit) => {
                        this.out_queue.push_back((transmit.destination, transmit.contents.into()));
                    }
                    Output::Event(event) => match event {
                        str0m::Event::Connected => {
                            log::info!("[WebrtcSession {}] connected", this.id);
                            this.connected = true;
                        }
                        str0m::Event::MediaAdded(media_added) => match media_added.kind {
                            str0m::media::MediaKind::Audio => {
                                log::info!("[WebrtcSession {}] audio media not supported", this.id);
                            }
                            str0m::media::MediaKind::Video => {
                                log::info!("[WebrtcSession {}] setup mid {}", this.id, media_added.mid);
                                this.video_mid = Some(media_added.mid);
                            }
                        },
                        str0m::Event::MediaData(data) => {
                            log::info!("[WebrtcSession {}] media data: {data:?}", this.id);
                            if Some(data.mid) == this.video_mid {
                                this.video_pt = Some(data.pt);
                            }
                        }
                        str0m::Event::Closed => {
                            log::info!("[WebrtcSession {}] closed", this.id);
                            this.connected = false;
                        }
                        other => {
                            log::info!("[WebrtcSession {}] event {other:?}", this.id);
                        }
                    },
                },
                Err(err) => {
                    log::error!("[WebrtcSession {}] poll_output error: {err:?}", this.id);
                    return Poll::Ready(None);
                }
            }
        }
        while let Some((target, buf)) = this.out_queue.front() {
            match this.udp.poll_send_to(cx, buf, *target) {
                Poll::Ready(Ok(_)) => {
                    // log::info!("[WebrtcSession {}] send {} bytes to {target}", this.id, buf.len());
                    this.out_queue.pop_front();
                }
                Poll::Ready(Err(e)) => {
                    log::error!("[WebrtcSession {}] udp send to error: {e:?}", this.id);
                    this.out_queue.pop_front();
                }
                Poll::Pending => {
                    break;
                }
            }
        }
        if let Some(timeout) = this.timeout.as_mut() {
            if let Poll::Ready(()) = timeout.poll_unpin(cx) {
                this.timeout = None;
                if let Err(e) = this.rtc.handle_input(Input::Timeout(Instant::now())) {
                    log::error!("[WebrtcSession {}] rtc handle_input timeout error: {e:?}", this.id);
                }
                cx.waker().wake_by_ref();
            }
        }

        Poll::Pending
    }
}
