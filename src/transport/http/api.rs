use std::{net::SocketAddr, time::Instant};

use axum::{Json, Router, extract::State, http::StatusCode, response::IntoResponse, routing::post};
use serde::{Deserialize, Serialize};
use str0m::{
    Candidate, Rtc, RtcError,
    change::SdpOffer,
    error::{IceError, SdpError},
    media::Pt,
    net::Protocol,
};
use thiserror::Error;
use tokio::{net::UdpSocket, sync::mpsc};

use crate::{
    session::webrtc_session::WebrtcSession,
    transport::http::{HttpOutput, context::HttpContext},
};

#[derive(Debug, Error)]
pub enum Error {
    #[error("Sdp Error: {0:?}")]
    SdpError(#[from] SdpError),
    #[error("Rtc Error: {0:?}")]
    RtcError(#[from] RtcError),
    #[error("Ice Error: {0:?}")]
    IceError(#[from] IceError),
    #[error("io error: {0:?}")]
    IOError(#[from] std::io::Error),
    #[error("channel error: {0:?}")]
    ChannelError(#[from] mpsc::error::SendError<HttpOutput>),
}

impl IntoResponse for Error {
    fn into_response(self) -> axum::response::Response {
        match self {
            Error::SdpError(sdp_error) => (StatusCode::BAD_REQUEST, sdp_error.to_string()).into_response(),
            Error::RtcError(rtc_error) => (StatusCode::INTERNAL_SERVER_ERROR, rtc_error.to_string()).into_response(),
            Error::IceError(ice_error) => (StatusCode::INTERNAL_SERVER_ERROR, ice_error.to_string()).into_response(),
            Error::IOError(error) => (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()).into_response(),
            Error::ChannelError(send_error) => (StatusCode::INTERNAL_SERVER_ERROR, send_error.to_string()).into_response(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct OfferRequest {
    sdp: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct OfferResponse {
    sdp: String,
}

pub fn router(state: HttpContext) -> Router {
    Router::new().route("/offer", post(offer)).with_state(state)
}

fn h264_payload_type_from_sdp(sdp: &str) -> Option<u8> {
    let mut h264_payload_types = Vec::new();
    let mut packetization_mode_one = Vec::new();

    for line in sdp.lines().map(str::trim) {
        if let Some(rest) = line.strip_prefix("a=rtpmap:") {
            let Some((pt, codec)) = rest.split_once(' ') else {
                continue;
            };
            if codec.eq_ignore_ascii_case("H264/90000") {
                if let Ok(pt) = pt.parse::<u8>() {
                    h264_payload_types.push(pt);
                }
            }
        } else if let Some(rest) = line.strip_prefix("a=fmtp:") {
            let Some((pt, params)) = rest.split_once(' ') else {
                continue;
            };
            if params.split(';').map(str::trim).any(|param| param == "packetization-mode=1") {
                if let Ok(pt) = pt.parse::<u8>() {
                    packetization_mode_one.push(pt);
                }
            }
        }
    }

    h264_payload_types
        .iter()
        .copied()
        .find(|pt| packetization_mode_one.contains(pt))
        .or_else(|| h264_payload_types.first().copied())
}

pub async fn offer(State(context): State<HttpContext>, Json(offer): Json<OfferRequest>) -> Result<Json<OfferResponse>, Error> {
    let mut rtc = Rtc::builder().clear_codecs().enable_h264(true).build(Instant::now());
    let udp = UdpSocket::bind(SocketAddr::new(context.addr, 10000)).await?;
    let address = udp.local_addr()?;
    rtc.add_local_candidate(Candidate::host(address, Protocol::Udp)?);
    let offer = SdpOffer::from_sdp_string(&offer.sdp)?;
    let answer = rtc.sdp_api().accept_offer(offer)?;
    let response = OfferResponse { sdp: answer.to_sdp_string() };
    let video_pt = h264_payload_type_from_sdp(&response.sdp).map(Pt::new_with_value);
    context.tx.send(HttpOutput::Webrtc(WebrtcSession::new(rtc, udp, address, video_pt))).await?;
    Ok(Json(response))
}

#[cfg(test)]
mod tests {
    #[test]
    fn extracts_packetization_mode_one_h264_payload_type_from_sdp() {
        let sdp = "\
v=0\r\n\
m=video 9 UDP/TLS/RTP/SAVPF 96 97 108 109\r\n\
a=rtpmap:96 VP8/90000\r\n\
a=rtpmap:97 rtx/90000\r\n\
a=fmtp:97 apt=96\r\n\
a=rtpmap:108 H264/90000\r\n\
a=fmtp:108 level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=42e01f\r\n\
a=rtpmap:109 rtx/90000\r\n\
a=fmtp:109 apt=108\r\n";

        assert_eq!(super::h264_payload_type_from_sdp(sdp), Some(108));
    }
}
