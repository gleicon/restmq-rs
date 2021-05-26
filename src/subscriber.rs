use std::pin::Pin;
use std::task::{Context, Poll};
use futures::{Stream};
use tokio::sync::mpsc::UnboundedReceiver;
use actix_web::web::Bytes;
use actix_web::{Result, Error};

pub struct SubscriberChannel(pub UnboundedReceiver<Bytes>);

impl Stream for SubscriberChannel {
    type Item = Result<Bytes, Error>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>, ) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.0).poll_recv(cx) {
            Poll::Ready(Some(v)) => Poll::Ready(Some(Ok(v))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}


