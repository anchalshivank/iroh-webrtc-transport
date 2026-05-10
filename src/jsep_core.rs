//! WebRTC JSEP negotiation (offer/answer + ICE-in-SDP) over any [`Signaling`] transport.
//!
//! Native WebRTC uses [str0m](https://github.com/algesten/str0m) (Sans-IO) on a local UDP socket.
//! Wire types: [`crate::jsep_envelope`], [`crate::jsep_signaling`]. Implementations:
//! [`crate::QuicSignaling`], [`crate::TcpWebSocket`].

use std::time::Instant;

use anyhow::{Context as _, bail};

use crate::jsep_envelope::SignalEnvelope;
use crate::jsep_signaling::Signaling;
use crate::str0m_peer::{
    ChannelReadyTarget, Str0mPeer, add_host_candidate, bind_ephemeral_udp, build_rtc,
    drive_until_channel_ready, sdp_answer_from_string, sdp_offer_from_string,
};

pub async fn negotiate_dc_as_offerer<S: Signaling + ?Sized>(
    sig: &mut S,
    dc_label: &str,
) -> anyhow::Result<Str0mPeer> {
    let now = Instant::now();
    let mut rtc = build_rtc(now);
    let (socket, advertised) = bind_ephemeral_udp().await.context("bind WebRTC UDP")?;
    add_host_candidate(&mut rtc, advertised).context("add ICE host candidate")?;

    let mut change = rtc.sdp_api();
    let channel_id = change.add_channel(dc_label.into());
    let (offer, pending) = change
        .apply()
        .ok_or_else(|| anyhow::anyhow!("str0m sdp_api.apply returned None (offer)"))?;

    sig.send_envelope(&SignalEnvelope::Offer {
        sdp: offer.to_sdp_string(),
    })
    .await?;

    let answer_env = sig.recv_envelope().await?;
    let sdp = match answer_env {
        SignalEnvelope::Answer { sdp } => sdp,
        other => bail!("expected answer, got {:?}", other),
    };

    let answer = sdp_answer_from_string(&sdp).context("parse SDP answer")?;
    rtc.sdp_api()
        .accept_answer(pending, answer)
        .context("accept_answer")?;

    let mut answerer_cid = None;
    drive_until_channel_ready(
        &mut rtc,
        &socket,
        advertised,
        ChannelReadyTarget::Offerer(channel_id),
        &mut answerer_cid,
    )
    .await?;

    Ok(Str0mPeer {
        rtc,
        socket,
        channel_id,
        advertised_addr: advertised,
    })
}

pub async fn negotiate_dc_as_answerer<S: Signaling + ?Sized>(
    sig: &mut S,
) -> anyhow::Result<Str0mPeer> {
    let now = Instant::now();
    let mut rtc = build_rtc(now);
    let (socket, advertised) = bind_ephemeral_udp().await.context("bind WebRTC UDP")?;
    add_host_candidate(&mut rtc, advertised).context("add ICE host candidate")?;

    let offer_env = sig.recv_envelope().await?;
    let sdp = match offer_env {
        SignalEnvelope::Offer { sdp } => sdp,
        other => bail!("expected offer, got {:?}", other),
    };

    let offer = sdp_offer_from_string(&sdp).context("parse SDP offer")?;
    let answer = rtc
        .sdp_api()
        .accept_offer(offer)
        .context("accept_offer")?;

    sig.send_envelope(&SignalEnvelope::Answer {
        sdp: answer.to_sdp_string(),
    })
    .await?;

    let mut channel_id = None;
    drive_until_channel_ready(
        &mut rtc,
        &socket,
        advertised,
        ChannelReadyTarget::Answerer,
        &mut channel_id,
    )
    .await?;

    let channel_id = channel_id.context("no data channel from remote offer")?;

    Ok(Str0mPeer {
        rtc,
        socket,
        channel_id,
        advertised_addr: advertised,
    })
}
