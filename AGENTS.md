# Documentation

Documentation defaults to English: `README.md` and unsuffixed Markdown files are English. Provide complete Chinese translations in `README_zh.md` and `*_zh.md` in the same change. Keep commands, configuration keys, examples, links, and technical details synchronized. Do not retain `_en.md` aliases.

# Unreleased development

This project has no released versions. Apply agreed naming and interface changes directly. Remove superseded interfaces, configuration aliases, and migration fallbacks instead of retaining backward compatibility. Update callers, examples, tests, and bilingual documentation together.

# Plugin architecture

Prefer plugins for independently contributed dataset backends, file formats, structured-file browsing, operators, and other domain-specific capabilities. Adding an implementation of an established extension point must not require editing core registries, rebuilding the server, or changing clients.

Keep core responsibilities focused on public contracts, plugin discovery and lifecycle, transport, authorization, namespace validation, cancellation, and resource management. Built-in and external implementations should use the same extension contracts. Avoid format- or backend-specific branches in shared code; let plugins own and validate their configuration parameters.

Scientific formats may contain groups, tables, images, and multidimensional variables. Preserve those structures and advertise explicit capabilities; do not silently flatten arrays into records. Extend shared contracts when needed and document new representations before implementing them.

For extension work, provide registration instructions, configuration examples, and meaningful contract tests. Keep third-party dependencies in plugin modules where practical. Distinguish implemented extension points from planned ones; see `docs/plugins.md`.
