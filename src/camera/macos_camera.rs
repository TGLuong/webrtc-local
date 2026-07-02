use std::{collections::VecDeque, task::Poll};

use futures::Stream;
use nokhwa::{
    Buffer, Camera, nokhwa_initialize,
    pixel_format::YuyvFormat,
    utils::{CameraFormat, CameraIndex, FrameFormat, RequestedFormat, RequestedFormatType},
};
use openh264::{
    OpenH264API,
    encoder::{Encoder, EncoderConfig, IntraFramePeriod},
    formats::YUVSlices,
};
use rtc::rtp::{
    codec::h264::H264Payloader,
    packetizer::{Packetizer, new_packetizer},
    sequence::new_random_sequencer,
};
use tokio::sync::mpsc;

use crate::transport::rtp::RtpPacket;

pub struct MacosCamera {
    rx: mpsc::Receiver<Buffer>,
    encoder: Encoder,
    packetizer: Box<dyn Packetizer>,
    outputs: VecDeque<RtpPacket>,
}

impl MacosCamera {
    pub fn new() -> anyhow::Result<Self> {
        let (tx, rx) = mpsc::channel(10);
        // tokio::task::spawn_blocking(move || {
        //     nokhwa_initialize(|_| println!("camera permission granted"));
        //     let index = CameraIndex::Index(0);
        //     let requested = RequestedFormat::new::<YuyvFormat>(RequestedFormatType::Exact(CameraFormat::new_from(1920, 1080, FrameFormat::YUYV, 30)));
        //     if let Ok(mut camera) = Camera::new(index, requested) {
        //         if let Ok(()) = camera.open_stream() {
        //             loop {
        //                 match camera.frame() {
        //                     Ok(frame) => match tx.blocking_send(frame) {
        //                         Ok(()) => {}
        //                         Err(err) => {
        //                             log::error!("[MacosCamera] channel error: {err:?}");
        //                             break;
        //                         }
        //                     },
        //                     Err(err) => {
        //                         log::error!("[MacosCamera] capture error: {err:?}");
        //                         break;
        //                     }
        //                 }
        //             }
        //         }
        //     }
        // });
        let config = EncoderConfig::new().intra_frame_period(IntraFramePeriod::from_num_frames(60));
        let api = OpenH264API::from_source();
        let encoder = Encoder::with_api_config(api, config)?;
        let payloader = H264Payloader::default();
        let packetizer = new_packetizer(1200, 109, rand::random(), Box::new(payloader), Box::new(new_random_sequencer()), 90_000);
        Ok(Self {
            rx,
            encoder,
            packetizer: Box::new(packetizer),
            outputs: VecDeque::default(),
        })
    }

    fn yuyv_to_yuv420p(buffer: &[u8], width: usize, height: usize) -> Option<(Vec<u8>, Vec<u8>, Vec<u8>)> {
        if width % 2 != 0 || height % 2 != 0 {
            return None;
        }
        if buffer.len() != width * height * 2 {
            return None;
        }
        let mut u_plane = Vec::with_capacity((width / 2) * (height / 2));
        let mut v_plane = Vec::with_capacity((width / 2) * (height / 2));
        let mut y_plane = vec![0; width * height];

        for row_pair in 0..(height / 2) {
            let top_row = row_pair * 2;
            let bottom_row = top_row + 1;

            for pair in 0..(width / 2) {
                let top_offset = top_row * width * 2 + pair * 4;
                let bottom_offset = bottom_row * width * 2 + pair * 4;

                let x = pair * 2;

                y_plane[top_row * width + x] = buffer[top_offset];
                y_plane[top_row * width + x + 1] = buffer[top_offset + 2];

                y_plane[bottom_row * width + x] = buffer[bottom_offset];
                y_plane[bottom_row * width + x + 1] = buffer[bottom_offset + 2];

                let u1 = buffer[top_offset + 1] as u16;
                let v1 = buffer[top_offset + 3] as u16;
                let u2 = buffer[bottom_offset + 1] as u16;
                let v2 = buffer[bottom_offset + 3] as u16;

                u_plane.push(((u1 + u2) / 2) as u8);
                v_plane.push(((v1 + v2) / 2) as u8);
            }
        }
        Some((y_plane, u_plane, v_plane))
    }
}

impl Stream for MacosCamera {
    type Item = RtpPacket;

    fn poll_next(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Option<Self::Item>> {
        let this = self.get_mut();
        while let Poll::Ready(event) = this.rx.poll_recv(cx) {
            match event {
                Some(buffer) => {
                    let width = buffer.resolution().width() as usize;
                    let height = buffer.resolution().height() as usize;
                    if let Some((y, u, v)) = Self::yuyv_to_yuv420p(buffer.buffer(), width, height) {
                        let yuv = YUVSlices::new((&y, &u, &v), (width, height), (width, width / 2, width / 2));
                        if let Ok(h264_bitstream) = this.encoder.encode(&yuv) {
                            if let Ok(packets) = this.packetizer.packetize(&h264_bitstream.to_vec().into(), 3000) {
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
                }
                None => return Poll::Ready(None),
            }
        }
        if let Some(out) = this.outputs.pop_front() {
            return Poll::Ready(Some(out));
        }
        Poll::Pending
    }
}
