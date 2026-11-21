/*-------------------------------------------------------------------------
 * Copyright (c) Microsoft Corporation.  All rights reserved.
 *
 * documentdb_gateway_core/src/responses/writer.rs
 *
 *-------------------------------------------------------------------------
 */

use std::io::IoSlice;

use bson::RawDocument;
use tokio::io::{AsyncWrite, AsyncWriteExt};

use crate::{
    context::ConnectionContext,
    error::{DocumentDBError, Result},
    protocol::{header::Header, opcode::OpCode},
    responses::{CommandError, Response},
};

const OP_MSG_PREFIX_LENGTH: usize =
    Header::LENGTH + std::mem::size_of::<u32>() + std::mem::size_of::<u8>();
const OP_REPLY_PREFIX_LENGTH: usize = Header::LENGTH + 20;

/// Writes a wire header immediately followed by a BSON body using vectored I/O.
///
/// A MongoDB wire response is a fixed-size header followed by the BSON body. Issuing a
/// single `write_vectored` lets the socket layer copy both buffers into one send, instead
/// of two separate `write_all` calls that each enter the async write path and can produce
/// two syscall/socket-write round trips per response. On the hot path (one response per
/// request) this removes one async write + syscall per request.
///
/// `write_vectored` is allowed to perform a partial write (and sinks without vectored
/// support write only the first non-empty buffer), so loop until every byte is written,
/// trimming the consumed prefix of the leading buffer on each iteration. The wire bytes
/// produced are byte-for-byte identical to two sequential `write_all` calls.
async fn write_header_and_body<S>(
    header: &[u8],
    body: &[u8],
    stream: &mut S,
) -> Result<()>
where
    S: AsyncWrite + Unpin,
{
    let mut head = header;
    let mut tail = body;
    while !head.is_empty() || !tail.is_empty() {
        let slices = [IoSlice::new(head), IoSlice::new(tail)];
        let written = stream.write_vectored(&slices).await?;
        if written == 0 {
            return Err(DocumentDBError::internal_error(
                "write_vectored made no progress".to_owned(),
            ));
        }
        // Consume `written` bytes from head first, then tail.
        let mut remaining = written;
        if remaining >= head.len() {
            remaining -= head.len();
            head = &[];
        } else {
            head = &head[remaining..];
            remaining = 0;
        }
        if remaining > 0 {
            tail = &tail[remaining..];
        }
    }
    Ok(())
}

/// Write a server response to the client stream
/// # Errors
/// Returns error if the operation fails.
pub async fn write<S>(header: &Header, response: &Response, stream: &mut S) -> Result<()>
where
    S: AsyncWrite + Unpin,
{
    write_and_flush(header, response.as_raw_document()?, stream).await
}

/// Write a raw BSON object to the client stream
/// # Errors
/// Returns error if the operation fails.
#[expect(clippy::cast_possible_truncation, reason = "message size fits in i32")]
#[expect(clippy::cast_possible_wrap, reason = "message size is always positive")]
pub async fn write_and_flush<S>(
    header: &Header,
    response: &RawDocument,
    stream: &mut S,
) -> Result<()>
where
    S: AsyncWrite + Unpin,
{
    // The format of the response will depend on the OP which the client sent
    match header.op_code() {
        OpCode::Command => unimplemented!(),

        // Messages are always responded to with messages
        OpCode::Msg => write_message(header, response, stream).await,

        // Query is responded to with Reply
        #[expect(
            deprecated,
            reason = "OP_QUERY is still supported for legacy clients and testing"
        )]
        OpCode::Query => {
            let message_length = (response.as_bytes().len() + OP_REPLY_PREFIX_LENGTH) as i32;

            let header = Header::new(
                // Total size of the response is the bytes + standard header + reply header
                message_length,
                header.request_id(),
                header.request_id(),
                OpCode::Reply,
            )?;

            // Serialize the OP_REPLY header section into one buffer:
            // 16-byte wire header + responseFlags(4) + cursorId(8) +
            // startingFrom(4) + numberReturned(4).
            let mut buf = vec![0_u8; OP_REPLY_PREFIX_LENGTH];
            buf[0..4].copy_from_slice(&message_length.to_le_bytes());
            buf[4..8].copy_from_slice(&header.request_id().to_le_bytes());
            buf[8..12].copy_from_slice(&header.request_id().to_le_bytes());
            buf[12..16].copy_from_slice(&(OpCode::Reply as i32).to_le_bytes());
            buf[16..20].copy_from_slice(&0_i32.to_le_bytes()); // Response flags
            buf[20..28].copy_from_slice(&0_i64.to_le_bytes()); // Cursor Id
            buf[28..32].copy_from_slice(&0_i32.to_le_bytes()); // startingFrom
            buf[32..36].copy_from_slice(&1_i32.to_le_bytes()); // numberReturned

            write_header_and_body(&buf, response.as_bytes(), stream).await?;
            Ok(())
        }

        // Insert has no response
        #[expect(
            deprecated,
            reason = "OP_INSERT is still supported for legacy clients and testing"
        )]
        OpCode::Insert => Ok(()),
        _ => Err(DocumentDBError::internal_error(format!(
            "Unexpected response opcode: {:?}",
            header.op_code()
        ))),
    }?;

    stream.flush().await?;

    Ok(())
}

/// Serializes the Message to bytes and writes them to `writer`.
/// # Errors
/// Returns error if the operation fails.
#[expect(clippy::cast_possible_truncation, reason = "message size fits in i32")]
#[expect(clippy::cast_possible_wrap, reason = "message size is always positive")]
pub async fn write_message<S>(header: &Header, response: &RawDocument, writer: &mut S) -> Result<()>
where
    S: AsyncWrite + Unpin,
{
    let total_length = Header::LENGTH
        + std::mem::size_of::<u32>()
        + std::mem::size_of::<u8>()
        + response.as_bytes().len(); // Message payload + status code

    let header = Header::new(
        total_length as i32,
        header.request_id(),
        header.request_id(),
        OpCode::Msg,
    )?;

    // Serialize the OP_MSG header section into one buffer:
    // 16-byte wire header + flags(4) + payload type(1).
    let mut buf = vec![0_u8; OP_MSG_PREFIX_LENGTH];
    buf[0..4].copy_from_slice(&(total_length as i32).to_le_bytes());
    buf[4..8].copy_from_slice(&header.request_id().to_le_bytes());
    buf[8..12].copy_from_slice(&header.request_id().to_le_bytes());
    buf[12..16].copy_from_slice(&(OpCode::Msg as i32).to_le_bytes());
    buf[16..20].copy_from_slice(&0_u32.to_le_bytes()); // Flags
    buf[20] = 0_u8; // Payload type + section (body)

    write_header_and_body(&buf, response.as_bytes(), writer).await?;

    Ok(())
}

/// # Errors
/// Returns error if the operation fails.
#[expect(clippy::cast_possible_truncation, reason = "message size fits in i32")]
#[expect(clippy::cast_possible_wrap, reason = "message size is always positive")]
pub async fn write_error_without_header<S>(
    connection_context: &ConnectionContext,
    err: DocumentDBError,
    stream: &mut S,
    activity_id: &str,
) -> Result<()>
where
    S: AsyncWrite + Unpin,
{
    let response =
        CommandError::from_error(connection_context, &err, activity_id).to_raw_document_buf();

    let header = Header::new((response.as_bytes().len() + 1) as i32, 0, 0, OpCode::Msg)?;

    write_and_flush(&header, &response, stream).await?;

    Ok(())
}
