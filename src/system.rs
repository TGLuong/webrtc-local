use std::task::Poll;

use futures::{Stream, StreamExt};

use crate::{
    Args,
    camera::macos_camera::MacosCamera,
    rtsp::RtspPoll,
    session::{SessionManager, WebrtcSessionEvent},
    transport::http::{Http, HttpOutput, context::HttpContext},
};

pub struct System {
    http: Http,
    sessions: SessionManager,
    camera: MacosCamera,
    rtsp: RtspPoll,
}

impl System {
    pub async fn new(args: Args) -> anyhow::Result<Self> {
        let (http_context, rx) = HttpContext::new(args.udp_addr);
        let http = Http::new(args.http_addr, rx, http_context).await?;
        let camera = MacosCamera::new()?;
        let rtsp = RtspPoll::new();
        Ok(Self {
            http,
            sessions: SessionManager::new(),
            camera,
            rtsp,
        })
    }
}

impl Stream for System {
    type Item = ();

    fn poll_next(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Option<Self::Item>> {
        let this = self.get_mut();

        while let Poll::Ready(out) = this.http.poll_next_unpin(cx) {
            match out {
                Some(event) => match event {
                    HttpOutput::Webrtc(webrtc_session) => {
                        this.sessions.insert(webrtc_session);
                    }
                    HttpOutput::Rtsp(rtsp_session) => {
                        this.rtsp.insert(rtsp_session);
                    }
                },
                None => return Poll::Ready(None),
            }
        }
        while let Poll::Ready(out) = this.sessions.poll_next_unpin(cx) {
            match out {
                Some(event) => match event {
                    WebrtcSessionEvent::Connected(uuid) => this.rtsp.start(uuid),
                    WebrtcSessionEvent::Closed(uuid) => this.rtsp.stop(uuid),
                },
                None => return Poll::Ready(None),
            }
        }
        // while let Poll::Ready(event) = this.camera.poll_next_unpin(cx) {
        //     match event {
        //         Some(rtp) => this.sessions.on_rtp(rtp),
        //         None => return Poll::Ready(None),
        //     }
        // }
        while let Poll::Ready(event) = this.rtsp.poll_next_unpin(cx) {
            match event {
                Some((id, rtp)) => this.sessions.on_session_rtp(id, rtp),
                None => return Poll::Ready(None),
            }
        }
        Poll::Pending
    }
}
