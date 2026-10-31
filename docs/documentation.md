# Documentation ownership

[中文](documentation_zh.md)

Each topic has one editable source. README files introduce the project, provide a minimal quick start and link to detailed guides; they do not duplicate complete manuals.

| Owner | Canonical content |
|---|---|
| dacp-codex | Data services, clients, Union, proxy, security, deployment and integration |
| xstream-codex | Engine, Job YAML, execution policies, Designer, data SDK and operator development |
| xstream-operator-repository | Repository APIs, submission review, administration, browsing and runtime catalog |
| Website repositories | Product presentation, downloads, screenshots, website deployment and demo operation |

English Markdown and complete Chinese translations live together in the owning repository. Module README files document module-specific setup; shared concepts belong in the project guides. Cross-project integrations explain their own behavior and link to the owner's reference rather than reproducing it.

Websites generate complete English manuals from versioned snapshots of these sources. Snapshots and HTML are build inputs/outputs, not independent authoring locations. Edit the owner first, refresh snapshots and regenerate HTML in the same change. SOURCE records the source commit; SNAPSHOT.json records exact working-tree content hashes. Generation rejects edits made directly to snapshots. A source checkout is needed only to refresh documentation, never to serve the site.

Keep short homepage introductions and quick-start examples where useful. Maintain detailed configuration, API contracts and examples only at their canonical source. Changes to examples must also update callers and bilingual references. Validate generated links and review snapshots together with source changes.
