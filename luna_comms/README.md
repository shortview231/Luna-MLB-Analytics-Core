# Luna_Comms

Luna_Comms is the outbound communication subsystem for Luna_Export.

It owns outbound job preparation, queueing, sending, retries, dedupe, and traceability for:

- Gmail report delivery
- Google Calendar outbound sync
- Notification/update feeds for cross-device visibility

Runtime state is JSON-first under `exports/staged/luna_comms/`.

Portfolio demo references:

- `luna_comms/docs/portfolio-demo.md`
- `luna_comms/docs/resume-snippets.md`
