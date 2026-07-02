use std::{
    collections::{HashMap, HashSet},
    task::Poll,
};

use futures::{Stream, StreamExt};
use uuid::Uuid;

use crate::{rtsp::rtsp_session::RtspSession, transport::rtp::RtpPacket};

pub mod rtsp_session;

pub struct RtspPoll {
    sessions: HashMap<Uuid, RtspSession>,
}

impl RtspPoll {
    pub fn new() -> Self {
        Self { sessions: HashMap::default() }
    }

    pub fn insert(&mut self, session: RtspSession) {
        self.sessions.insert(session.id(), session);
    }

    pub fn start(&mut self, id: Uuid) {
        if let Some(session) = self.sessions.get_mut(&id) {
            session.start();
        }
    }

    pub fn stop(&mut self, id: Uuid) {
        if let Some(session) = self.sessions.get_mut(&id) {
            session.stop();
        }
    }
}

impl Stream for RtspPoll {
    type Item = (Uuid, RtpPacket);

    fn poll_next(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let mut ended = HashSet::new();
        for (id, session) in this.sessions.iter_mut() {
            while let Poll::Ready(out) = session.poll_next_unpin(cx) {
                match out {
                    Some(out) => return Poll::Ready(Some(out)),
                    None => {
                        ended.insert(id.clone());
                    }
                }
            }
        }
        this.sessions.retain(|k, _| !ended.contains(k));
        Poll::Pending
    }
}
