use std::{
    collections::{HashMap, HashSet},
    task::Poll,
};

use futures::{Stream, StreamExt};
use uuid::Uuid;

use crate::{session::webrtc_session::WebrtcSession, transport::rtp::RtpPacket};

pub mod webrtc_session;

#[derive(Debug)]
pub enum WebrtcSessionEvent {
    Connected(Uuid),
    Closed(Uuid),
}

pub struct SessionManager {
    sessions: HashMap<Uuid, WebrtcSession>,
}

impl SessionManager {
    pub fn new() -> Self {
        Self { sessions: HashMap::default() }
    }

    pub fn insert(&mut self, session: WebrtcSession) {
        log::info!("[SessionManager] add new session {}", session.id());
        self.sessions.insert(session.id(), session);
    }

    pub fn on_rtp(&mut self, rtp: RtpPacket) {
        for session in self.sessions.values_mut() {
            if let Err(e) = session.send_video(rtp.clone()) {
                log::error!("[SessionManager] session {} send video error: {e:?}", session.id());
            }
        }
    }

    pub fn on_session_rtp(&mut self, id: Uuid, rtp: RtpPacket) {
        if let Some(session) = self.sessions.get_mut(&id) {
            if let Err(err) = session.send_video(rtp) {
                log::error!("[SessionManager] session {} send video error: {err:?}", session.id());
            }
        }
    }
}

impl Stream for SessionManager {
    type Item = WebrtcSessionEvent;

    fn poll_next(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let mut ended = HashSet::new();
        for session in this.sessions.values_mut() {
            while let Poll::Ready(event) = session.poll_next_unpin(cx) {
                match event {
                    Some(event) => return Poll::Ready(Some(event)),
                    None => {
                        ended.insert(session.id());
                    }
                }
            }
        }
        this.sessions.retain(|id, _| !ended.contains(id));

        Poll::Pending
    }
}
