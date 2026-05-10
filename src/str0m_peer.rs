//! [str0m](https://github.com/algesten/str0m) peer connection + SCTP data channel, driven on a UDP socket.
//!
//! Sans-IO `Rtc` is polled in a Tokio task when attached to [`crate::bridge::WebRtcTunnel`] or user line I/O.

use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Context as _;
use iroh_base::CustomAddr;
use str0m::change::{SdpAnswer, SdpOffer};
use str0m::channel::ChannelId;
use str0m::net::{Protocol, Receive};
use str0m::{Candidate, Event, Input, Output, Rtc, RtcError};
use tokio::net::UdpSocket;

use crate::bridge::{AttachOptions, InboundPacket, WebRtcTunnel};

/// Crypto + `Rtc` builder shared by JSEP negotiation.
pub(crate) fn build_rtc(now: Instant) -> Rtc {
    Rtc::builder()
        .set_crypto_provider(Arc::new(str0m_aws_lc_rs::default_provider()))
        .build(now)
}

pub(crate) async fn bind_ephemeral_udp() -> std::io::Result<(UdpSocket, SocketAddr)> {
    let socket = UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 0)).await?;
    let local = socket.local_addr()?;
    let ip = if local.ip().is_unspecified() {
        if_addrs::get_if_addrs()
            .ok()
            .into_iter()
            .flatten()
            .find(|iface| !iface.is_loopback() && iface.addr.ip().is_ipv4())
            .map(|iface| iface.addr.ip())
            .unwrap_or(Ipv4Addr::LOCALHOST.into())
    } else {
        local.ip()
    };
    let advertised = SocketAddr::new(ip, local.port());
    Ok((socket, advertised))
}

pub(crate) fn add_host_candidate(rtc: &mut Rtc, advertised: SocketAddr) -> anyhow::Result<()> {
    let c = Candidate::host(advertised, "udp").context("ICE host candidate")?;
    rtc.add_local_candidate(c);
    Ok(())
}

/// A negotiated WebRTC data channel backed by str0m (ready for I/O).
pub struct Str0mPeer {
    pub(crate) rtc: Rtc,
    pub(crate) socket: UdpSocket,
    pub(crate) channel_id: ChannelId,
    pub(crate) advertised_addr: SocketAddr,
}

impl Str0mPeer {
    /// For `iroh-jsep-chat` / `ws-chat`: spawn the UDP/`Rtc` loop and return user↔channel byte pipes.
    pub fn spawn_line_io_bridge(
        self,
    ) -> (
        tokio::sync::mpsc::UnboundedSender<Vec<u8>>,
        tokio::sync::mpsc::UnboundedReceiver<Vec<u8>>,
    ) {
        let (user_tx, user_rx) = tokio::sync::mpsc::unbounded_channel();
        let (peer_tx, peer_rx) = tokio::sync::mpsc::unbounded_channel();
        tokio::spawn(str0m_user_bridge_loop(self, user_rx, peer_tx));
        (user_tx, peer_rx)
    }
}

/// Drive `Rtc` until the SCTP data channel is open (`ChannelOpen` for `target` or any channel on answerer).
pub(crate) async fn drive_until_channel_ready(
    rtc: &mut Rtc,
    socket: &UdpSocket,
    advertised_addr: SocketAddr,
    target: ChannelReadyTarget,
    answerer_cid: &mut Option<ChannelId>,
) -> anyhow::Result<()> {
    let deadline = Instant::now() + Duration::from_secs(60);
    let mut buf = vec![0u8; 2000];
    let mut next_wake = Instant::now();
    let mut opened = false;

    while Instant::now() < deadline && !opened {
        let sleep_dur = next_wake
            .saturating_duration_since(Instant::now())
            .max(Duration::from_millis(1))
            .min(Duration::from_millis(50));

        tokio::select! {
            _ = tokio::time::sleep(sleep_dur) => {
                let now = Instant::now();
                rtc.handle_input(Input::Timeout(now))
                    .map_err(|e| anyhow::anyhow!("{e}"))?;
                next_wake = drain_handshake_outputs(
                    rtc,
                    socket,
                    advertised_addr,
                    &mut buf,
                    target,
                    answerer_cid,
                    &mut opened,
                )
                .await
                .map_err(|e| anyhow::anyhow!("{e}"))?;
            }
            r = socket.recv_from(&mut buf) => {
                let (n, src) = r.context("UDP recv (JSEP handshake)")?;
                if n == 0 {
                    continue;
                }
                let now = Instant::now();
                let recv = Receive::new(Protocol::Udp, src, advertised_addr, &buf[..n])
                    .map_err(|e| anyhow::anyhow!("{e}"))?;
                rtc.handle_input(Input::Receive(now, recv))
                    .map_err(|e| anyhow::anyhow!("{e}"))?;
                next_wake = drain_handshake_outputs(
                    rtc,
                    socket,
                    advertised_addr,
                    &mut buf,
                    target,
                    answerer_cid,
                    &mut opened,
                )
                .await
                .map_err(|e| anyhow::anyhow!("{e}"))?;
            }
        }
    }

    if !opened {
        anyhow::bail!("timeout waiting for WebRTC data channel to open");
    }
    Ok(())
}

#[derive(Clone, Copy)]
pub(crate) enum ChannelReadyTarget {
    Offerer(ChannelId),
    Answerer,
}

async fn drain_handshake_outputs(
    rtc: &mut Rtc,
    socket: &UdpSocket,
    _advertised_addr: SocketAddr,
    _buf: &mut [u8],
    target: ChannelReadyTarget,
    answerer_cid: &mut Option<ChannelId>,
    opened: &mut bool,
) -> Result<Instant, RtcError> {
    loop {
        match rtc.poll_output()? {
            Output::Timeout(t) => return Ok(t),
            Output::Transmit(tr) => {
                let _ = socket.send_to(&*tr.contents, tr.destination).await;
            }
            Output::Event(Event::ChannelOpen(id, _)) => match target {
                ChannelReadyTarget::Offerer(expected) if id == expected => {
                    *opened = true;
                }
                ChannelReadyTarget::Answerer => {
                    if answerer_cid.is_none() {
                        *answerer_cid = Some(id);
                        *opened = true;
                    }
                }
                _ => {}
            },
            Output::Event(Event::ChannelData(_)) => {}
            Output::Event(_) => {}
        }
    }
}

async fn str0m_user_bridge_loop(
    peer: Str0mPeer,
    mut from_user: tokio::sync::mpsc::UnboundedReceiver<Vec<u8>>,
    to_user: tokio::sync::mpsc::UnboundedSender<Vec<u8>>,
) {
    let Str0mPeer {
        mut rtc,
        socket,
        channel_id,
        advertised_addr,
    } = peer;
    let mut buf = vec![0u8; 2000];
    let mut next_wake = Instant::now();

    loop {
        while let Ok(bytes) = from_user.try_recv() {
            if bytes.is_empty() {
                continue;
            }
            loop {
                let mut wrote = false;
                if let Some(mut ch) = rtc.channel(channel_id) {
                    match ch.write(true, &bytes) {
                        Ok(true) => wrote = true,
                        Ok(false) => {}
                        Err(_) => return,
                    }
                }
                if wrote {
                    break;
                }
                next_wake = match drain_until_timeout(&mut rtc, &socket, advertised_addr, &mut buf).await {
                    Ok(t) => t,
                    Err(_) => return,
                };
            }
            let _ = drain_until_timeout(&mut rtc, &socket, advertised_addr, &mut buf).await;
        }

        let sleep_dur = next_wake
            .saturating_duration_since(Instant::now())
            .max(Duration::from_millis(1))
            .min(Duration::from_millis(100));

        tokio::select! {
            _ = tokio::time::sleep(sleep_dur) => {
                let now = Instant::now();
                if rtc.handle_input(Input::Timeout(now)).is_err() {
                    return;
                }
                next_wake = match process_outputs_user(
                    &mut rtc,
                    &socket,
                    advertised_addr,
                    &mut buf,
                    channel_id,
                    &to_user,
                )
                .await
                {
                    Ok(t) => t,
                    Err(_) => return,
                };
            }
            r = socket.recv_from(&mut buf) => {
                let Ok((n, src)) = r else { return };
                if n == 0 {
                    continue;
                }
                let now = Instant::now();
                let Ok(recv) = Receive::new(Protocol::Udp, src, advertised_addr, &buf[..n]) else {
                    continue;
                };
                if rtc.handle_input(Input::Receive(now, recv)).is_err() {
                    return;
                }
                next_wake = match process_outputs_user(
                    &mut rtc,
                    &socket,
                    advertised_addr,
                    &mut buf,
                    channel_id,
                    &to_user,
                )
                .await
                {
                    Ok(t) => t,
                    Err(_) => return,
                };
            }
        }
    }
}

async fn process_outputs_user(
    rtc: &mut Rtc,
    socket: &UdpSocket,
    _advertised_addr: SocketAddr,
    _buf: &mut [u8],
    channel_id: ChannelId,
    to_user: &tokio::sync::mpsc::UnboundedSender<Vec<u8>>,
) -> Result<Instant, RtcError> {
    loop {
        match rtc.poll_output()? {
            Output::Timeout(t) => return Ok(t),
            Output::Transmit(tr) => {
                let _ = socket.send_to(&*tr.contents, tr.destination).await;
            }
            Output::Event(Event::ChannelData(d)) if d.id == channel_id => {
                let _ = to_user.send(d.data);
            }
            Output::Event(_) => {}
        }
    }
}

async fn drain_until_timeout(
    rtc: &mut Rtc,
    socket: &UdpSocket,
    _advertised_addr: SocketAddr,
    _buf: &mut [u8],
) -> Result<Instant, RtcError> {
    loop {
        match rtc.poll_output()? {
            Output::Timeout(t) => return Ok(t),
            Output::Transmit(tr) => {
                let _ = socket.send_to(&*tr.contents, tr.destination).await;
            }
            Output::Event(Event::ChannelData(_)) => {}
            Output::Event(_) => {}
        }
    }
}

pub(crate) fn sdp_offer_from_string(s: &str) -> anyhow::Result<SdpOffer> {
    SdpOffer::from_sdp_string(s).map_err(|e| anyhow::anyhow!("{e}"))
}

pub(crate) fn sdp_answer_from_string(s: &str) -> anyhow::Result<SdpAnswer> {
    SdpAnswer::from_sdp_string(s).map_err(|e| anyhow::anyhow!("{e}"))
}

impl WebRtcTunnel {
    pub(crate) fn attach_str0m_peer(
        self: &Arc<Self>,
        peer: Str0mPeer,
        remote_custom: CustomAddr,
        opts: AttachOptions,
    ) -> anyhow::Result<()> {
        self.try_mark_attached()
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        self.set_remote_custom(remote_custom.clone())
            .map_err(|e| anyhow::anyhow!("{e}"))?;

        let mut out_rx = self
            .take_outbound_receiver()
            .map_err(|e| anyhow::anyhow!("{e}"))?;

        let in_tx = self.inbound_sender();
        let wake = Arc::clone(self);
        let tap = opts.tap_inbound_to.clone();
        let mirror = opts.mirror_sctp_echo;

        let Str0mPeer {
            mut rtc,
            socket,
            channel_id,
            advertised_addr,
        } = peer;

        tokio::spawn(async move {
            let mut buf = vec![0u8; 2000];
            let mut next_wake = Instant::now();
            loop {
                while let Ok(bytes) = out_rx.try_recv() {
                    if bytes.is_empty() {
                        continue;
                    }
                    loop {
                        let mut wrote = false;
                        if let Some(mut ch) = rtc.channel(channel_id) {
                            match ch.write(true, &bytes) {
                                Ok(true) => wrote = true,
                                Ok(false) => {}
                                Err(_) => return,
                            }
                        }
                        if wrote {
                            break;
                        }
                        next_wake = match drain_until_timeout(&mut rtc, &socket, advertised_addr, &mut buf).await {
                            Ok(t) => t,
                            Err(_) => return,
                        };
                    }
                    let _ = drain_until_timeout(&mut rtc, &socket, advertised_addr, &mut buf).await;
                }

                let sleep_dur = next_wake
                    .saturating_duration_since(Instant::now())
                    .max(Duration::from_millis(1))
                    .min(Duration::from_millis(100));

                tokio::select! {
                    _ = tokio::time::sleep(sleep_dur) => {
                        let now = Instant::now();
                        if rtc.handle_input(Input::Timeout(now)).is_err() {
                            return;
                        }
                        next_wake = match process_outputs_tunnel(
                            &mut rtc,
                            &socket,
                            advertised_addr,
                            &mut buf,
                            channel_id,
                            &in_tx,
                            &wake,
                            &remote_custom,
                            mirror,
                            tap.as_ref(),
                        )
                        .await
                        {
                            Ok(t) => t,
                            Err(_) => return,
                        };
                    }
                    r = socket.recv_from(&mut buf) => {
                        let Ok((n, src)) = r else { return };
                        if n == 0 {
                            continue;
                        }
                        let now = Instant::now();
                        let Ok(recv) = Receive::new(Protocol::Udp, src, advertised_addr, &buf[..n]) else {
                            continue;
                        };
                        if rtc.handle_input(Input::Receive(now, recv)).is_err() {
                            return;
                        }
                        next_wake = match process_outputs_tunnel(
                            &mut rtc,
                            &socket,
                            advertised_addr,
                            &mut buf,
                            channel_id,
                            &in_tx,
                            &wake,
                            &remote_custom,
                            mirror,
                            tap.as_ref(),
                        )
                        .await
                        {
                            Ok(t) => t,
                            Err(_) => return,
                        };
                    }
                }
            }
        });

        Ok(())
    }
}

async fn process_outputs_tunnel(
    rtc: &mut Rtc,
    socket: &UdpSocket,
    advertised_addr: SocketAddr,
    buf: &mut [u8],
    channel_id: ChannelId,
    in_tx: &tokio::sync::mpsc::Sender<InboundPacket>,
    wake: &Arc<WebRtcTunnel>,
    remote_custom: &CustomAddr,
    mirror: bool,
    tap: Option<&tokio::sync::mpsc::UnboundedSender<Vec<u8>>>,
) -> Result<Instant, RtcError> {
    loop {
        match rtc.poll_output()? {
            Output::Timeout(t) => return Ok(t),
            Output::Transmit(tr) => {
                let _ = socket.send_to(&*tr.contents, tr.destination).await;
            }
            Output::Event(Event::ChannelData(d)) if d.id == channel_id => {
                let bytes = d.data;
                if let Some(t) = tap {
                    let _ = t.send(bytes.clone());
                }
                let pkt = InboundPacket {
                    source_custom: remote_custom.clone(),
                    payload: bytes.clone(),
                };
                if in_tx.send(pkt).await.is_ok() {
                    wake.wake_recv_pollers();
                }
                if mirror {
                    if let Some(mut ch) = rtc.channel(channel_id) {
                        let _ = ch.write(true, &bytes);
                    }
                    let _ = drain_until_timeout(rtc, socket, advertised_addr, buf).await;
                }
            }
            Output::Event(_) => {}
        }
    }
}
