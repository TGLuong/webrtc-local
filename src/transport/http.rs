use std::{net::SocketAddr, task::Poll};

use axum::Router;
use futures::Stream;
use tokio::{net::TcpListener, sync::mpsc};
use tower_http::services::ServeDir;

use crate::{session::webrtc_session::WebrtcSession, transport::http::context::HttpContext};

pub mod api;
pub mod context;

#[derive(Debug)]
pub enum HttpOutput {
    Webrtc(WebrtcSession),
}

#[derive(Debug)]
pub struct Http {
    rx: mpsc::Receiver<HttpOutput>,
}

impl Http {
    pub async fn new(http_addr: SocketAddr, rx: mpsc::Receiver<HttpOutput>, context: HttpContext) -> anyhow::Result<Self> {
        let router = router(context.clone());
        let listener = TcpListener::bind(http_addr).await?;
        tokio::spawn(async move {
            if let Err(e) = axum::serve(listener, router).await {
                log::error!("[Http] axum serve error: {e:?}");
            }
        });
        Ok(Self { rx })
    }
}

fn router(context: HttpContext) -> Router {
    Router::new()
        .merge(api::router(context))
        .fallback_service(ServeDir::new("html").append_index_html_on_directories(true))
}

impl Stream for Http {
    type Item = HttpOutput;

    fn poll_next(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if let Poll::Ready(event) = this.rx.poll_recv(cx) {
            return Poll::Ready(event);
        }
        Poll::Pending
    }
}

#[cfg(test)]
mod tests {
    use axum::{
        body::{Body, to_bytes},
        http::{Request, StatusCode},
    };
    use tower::ServiceExt;

    use crate::transport::http::context::HttpContext;

    #[tokio::test]
    async fn serves_index_html_from_html_directory() {
        let (context, _rx) = HttpContext::new("127.0.0.1".parse().unwrap());
        let response = super::router(context)
            .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body = String::from_utf8_lossy(&body);
        assert!(body.contains("<video"));
        assert!(body.contains("console.groupCollapsed"));
        assert!(body.contains("SDP request"));
        assert!(body.contains("SDP response"));
        assert!(body.contains("WebRTC stats"));
        assert!(body.contains("video element event"));
        assert!(body.contains("receiver parameters"));
    }
}
