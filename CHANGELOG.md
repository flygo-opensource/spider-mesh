# Changelog

## 3.0.0 — event ownership extracted from core

- Event transporter bắt buộc tự khai báo `readonly name`; bỏ class-name inference.

- Added `EventBus`, `EventTransporter`, class-name topic linking, explicit transporter selection,
  and opt-in fanout.
- Added a structural mesh metadata bridge for brokerless subscriber discovery.
- Added `NestJSLinkEvent`; the helper no longer belongs to `@spider-mesh/core`.
