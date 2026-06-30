# webrtc-local

`webrtc-local` is a small Rust experiment for streaming a local macOS camera to a browser over WebRTC.

The app captures frames from the native camera, converts them from YUYV to YUV420p, encodes them as H.264, packetizes the encoded bitstream into RTP, and sends the RTP payloads through a `str0m` WebRTC session. A minimal web page served by the app creates a recv-only WebRTC connection and displays the remote video in Chrome.

## What It Does

- Captures camera frames with `nokhwa`.
- Encodes video with `openh264`.
- Packetizes H.264 into RTP payloads.
- Handles WebRTC signaling through an HTTP `/offer` endpoint.
- Uses `str0m` for ICE, DTLS, SRTP, SDP, and RTP transmission.
- Serves a local browser UI from `html/index.html`.

## Architecture

```text
macOS camera
  -> YUYV frame
  -> YUV420p conversion
  -> OpenH264 encoder
  -> H.264 Annex-B bitstream
  -> H.264 RTP packetizer
  -> str0m WebRTC session
  -> Chrome video element
```

Main modules:

- `src/camera/macos_camera.rs` captures, converts, encodes, and packetizes camera frames.
- `src/session/webrtc_session.rs` owns a WebRTC session and writes RTP payloads into `str0m`.
- `src/session.rs` broadcasts camera RTP packets to active WebRTC sessions.
- `src/transport/http/api.rs` accepts browser SDP offers and returns SDP answers.
- `src/system.rs` ties the HTTP server, camera stream, and session manager together.
- `html/index.html` is the browser test page.

## Running

```sh
cargo run
```

By default the app listens on:

```text
http://127.0.0.1:8080
```

Open that URL in Chrome and click `Connect`.

The default UDP bind address is `127.0.0.1`, and the WebRTC UDP port is currently `10000`.

## Configuration

The binary accepts:

```sh
cargo run -- --http-addr 127.0.0.1:8080 --udp-addr 127.0.0.1
```

These can also be provided through environment variables:

```sh
HTTP_ADDR=127.0.0.1:8080 UDP_ADDR=127.0.0.1 cargo run
```

## Tests

```sh
cargo test
```

The tests cover the HTTP page, SDP H.264 payload type extraction, WebRTC RTP sequence-number initialization, and keeping the system stream alive after forwarding camera RTP.

## WebRTC Notes

H.264 RTP payload type must match the negotiated SDP answer. The app extracts the H.264 payload type from SDP and prefers `packetization-mode=1`, because the RTP stream uses H.264 packetization such as STAP-A and FU-A.

The first RTP sequence number must start with rollover counter `0`. Do not initialize outbound RTP with `SeqNo::new()`, because it creates a random 64-bit sequence value that can start with a non-zero ROC. Chrome will not know that initial ROC and may drop the RTP stream even when the WebRTC connection is otherwise connected.

## Debugging

Useful browser signals are available in Chrome DevTools and `getStats()`:

- `connectionState` should become `connected`.
- inbound video `packetsReceived` should increase.
- `framesDecoded` and `keyFramesDecoded` should increase.
- the video element should report non-zero `videoWidth` and `videoHeight`.

If WebRTC connects but no video appears, first check:

- whether inbound RTP packets are received by Chrome;
- whether the selected RTP payload type maps to negotiated H.264;
- whether the first RTP sequence number has ROC `0`;
- whether the app is still running after the first camera RTP packet.
