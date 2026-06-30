use std::task::Poll;

use futures::{Stream, StreamExt};

use crate::{
    Args,
    camera::macos_camera::MacosCamera,
    session::SessionManager,
    transport::http::{Http, HttpOutput, context::HttpContext},
};

pub struct System {
    http: Http,
    sessions: SessionManager,
    camera: MacosCamera,
}

impl System {
    pub async fn new(args: Args) -> anyhow::Result<Self> {
        let (http_context, rx) = HttpContext::new(args.udp_addr);
        let http = Http::new(args.http_addr, rx, http_context).await?;
        let camera = MacosCamera::new()?;
        Ok(Self {
            http,
            sessions: SessionManager::new(),
            camera,
        })
    }

    fn poll_result_after_camera_rtp() -> Poll<Option<()>> {
        Poll::Ready(Some(()))
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
                },
                None => return Poll::Ready(None),
            }
        }
        while let Poll::Ready(out) = this.sessions.poll_next_unpin(cx) {
            match out {
                Some(()) => {}
                None => return Poll::Ready(None),
            }
        }
        while let Poll::Ready(event) = this.camera.poll_next_unpin(cx) {
            match event {
                Some(rtp) => {
                    log::debug!("[System] rtp from camera: {rtp:?}");
                    this.sessions.on_rtp(rtp);
                    return Self::poll_result_after_camera_rtp();
                }
                None => return Poll::Ready(None),
            }
        }
        Poll::Pending
    }
}

#[cfg(test)]
mod tests {
    use std::task::Poll;

    #[test]
    fn keeps_system_stream_alive_after_forwarding_camera_rtp() {
        assert!(matches!(super::System::poll_result_after_camera_rtp(), Poll::Ready(Some(()))));
    }
}
