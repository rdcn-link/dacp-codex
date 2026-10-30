# Operator categories and icons

[中文](operator-presentation_zh.md)

Developers provide `icon`, `inputs` and `parameters`. Repository administrators assign **multiple category labels** during approval. Developer submissions must not contain `category` or `categories`. Approved public manifests use `categories: [Climate, Conversion]`; the singular `category` field has been removed.

```yaml
icon:
  path: M3 5h18v14H3zM3 10h18M9 5v14
  color: '#14866b'
inputs:
  source:
    representations: [records]
    formats: []
parameters:
  type: object
  properties: {}
  required: []
  additionalProperties: false
```

Approval requires 1–16 unique labels, each 1–80 characters without surrounding whitespace or control characters. Labels differing only in letter case cannot be repeated in one manifest. Administrators select existing labels or add new ones; no registry edits or server rebuilds are needed. Public search matches all assigned labels. The Designer lists an operator in every assigned category and retains the same operator reference when dragged from any category. Built-in `read_item` uses its built-in Data sources category.

`icon` contains only `path` and `color`. `path` uses SVG's 24×24 coordinate system, starts with M/m, contains at most 2048 characters and accepts only path commands and numeric characters. Multiple subpaths are allowed. `color` is `#RRGGBB`. Rendering uses rounded strokes without fill. Full SVG, scripts, HTML, external URLs and image files are not accepted. Validation checks syntax restrictions, not visual quality.

Public cards and detail pages also show the administrator-assessed 0–5 trust score. The original scan score and findings stay in the private review record. See [source-only submission and review](operator-submission.md). Shared validation lives in `dacp.operator.OperatorPresentation` in `dacp-operator-sdk/contract`.
