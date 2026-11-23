These sources are patched through the workspace manifest so a fresh checkout
can build the HTTP adapter without local paths outside the repository.

- axum-core is based on crates.io 0.5.6. The patch adds
  `Body::downcast_ref` and tests its behavior with wrapping and pinning.
- ureq-proto is based on the axum-proto branch at
  `5eb5df021823c91ab244b5d9c709db70af518763`, with the request/response
  split from local patch commit `45e320c`.

Each snapshot preserves the upstream license.
