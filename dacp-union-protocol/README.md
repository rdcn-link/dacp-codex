# Union transport protocol module

[中文](README_zh.md)

Provides shared JSON validation, persistence helpers, Arrow Flight mTLS listeners, and request tools for `dacp-server`, `dacp-union-server`, and the Scala client. Application identity is the TLS-verified leaf certificate SHA-256 fingerprint. Remote certificates can be pinned. JSON messages are limited to 1 MiB.

This module contains no dataset backends, file formats, or domain operators. See the [Union server](../dacp-union-server/README.md) for interfaces and operation, and the [protocol document](../docs/union.md) for authorization and streaming contracts. Contract tests in `dacp-union-server` exercise this module over real Arrow Flight mTLS.
