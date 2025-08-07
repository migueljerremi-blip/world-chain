//! A Tower service used as a RPC middleware on the [`OpEngineApiExt`] to extract
//! [`Authorization`] from rollup-boost on ForkChoiceState updates, and notify the jobs generator.

use std::{future::Future, task::Poll};

use alloy_rlp::Decodable;
use jsonrpsee::{
    core::BoxError,
    server::{HttpRequest, HttpResponse},
};
use rollup_boost::Authorization;
use tokio::sync::watch::Sender;
use tower::{Layer, Service};
use tracing::warn;

const FLASHBLOCKS_AUTHORIZATION_HEADER: &str = "flashblocks-authorization";

#[derive(Debug, Clone)]
pub struct AuthorizationLayer {
    to_jobs_generator: Sender<Authorization>,
}

impl AuthorizationLayer {
    pub fn new(to_jobs_generator: Sender<Authorization>) -> Self {
        Self { to_jobs_generator }
    }
}

impl<S> Layer<S> for AuthorizationLayer {
    type Service = AuthorizationService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        AuthorizationService::new(inner, self.to_jobs_generator.clone())
    }
}

#[derive(Debug, Clone)]
pub struct AuthorizationService<S> {
    inner: S,
    to_jobs_generator: Sender<Authorization>,
}

impl<S> AuthorizationService<S> {
    pub fn new(inner: S, to_jobs_generator: Sender<Authorization>) -> Self {
        Self {
            inner,
            to_jobs_generator,
        }
    }
}

impl<S> Service<HttpRequest> for AuthorizationService<S>
where
    S: Service<HttpRequest, Response = HttpResponse> + Send + Sync + Clone + 'static,
    S::Response: 'static,
    S::Error: Into<BoxError> + 'static,
    S::Future: Send + 'static,
{
    type Error = S::Error;
    type Future = S::Future;
    type Response = HttpResponse;

    fn poll_ready(&mut self, cx: &mut std::task::Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    /// Extracts the `flashblocks-authorization` header from the request, decode it, and send it to the jobs generator.
    /// If the header is not present than the request is not an authorized fork choice update.
    fn call(&mut self, req: HttpRequest) -> Self::Future {
        let headers = req.headers();
        if let Some(h) = headers.get(FLASHBLOCKS_AUTHORIZATION_HEADER) {
            match Authorization::decode(&mut h.as_bytes()) {
                Ok(auth) => {
                    self.to_jobs_generator.send_replace(auth);
                }
                Err(e) => {
                    warn!(target: "rpc_middleware", "Failed to decode flashblocks authorization header: {e}");
                }
            }
        };

        self.inner.call(req)
    }
}
