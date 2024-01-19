use core::task::{Context, Poll};
use std::{ops::Deref, pin::Pin};

use tokio::sync::mpsc;

pub struct DropReceiver<T> {
    pub inner: mpsc::Receiver<T>,
}

impl<T> tokio_stream::Stream for DropReceiver<T> {
    type Item = T;

    fn poll_next(mut self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<T>> {
        Pin::new(&mut self.inner).poll_recv(cx)
    }
}

impl<T> Deref for DropReceiver<T> {
    type Target = mpsc::Receiver<T>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<T> Drop for DropReceiver<T> {
    fn drop(&mut self) {}
}
