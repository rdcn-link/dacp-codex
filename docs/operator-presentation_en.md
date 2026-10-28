# Operator categories and icons

Every local `operator.yaml`, publication `--ports FILE` and HTTP publication manifest must include `category`, `icon`, `inputs` and `parameters`. Missing or invalid values fail publication, catalog reads and node loading. There are no legacy aliases or default categories.

```yaml
category: Data transformation
icon:
  path: "M3 5h18v14H3zM3 10h18M3 14h18M9 5v14"
  color: "#14866b"
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

`category` is an author-defined label of 1–80 characters, without surrounding whitespace or control characters. Identical labels share a category; comparison is case-sensitive. Use a domain or purpose such as `Climate analysis` or `Format conversion`. Categories require no core registry edits or server rebuilds.

`icon` contains only `path` and `color`. `path` uses SVG's 24×24 coordinate system, starts with M/m, has at most 2048 characters, and contains only path commands and numeric characters; multiple subpaths are allowed. `color` must be `#RRGGBB`. Rendering uses rounded strokes without fill. Do not submit full SVG, scripts, HTML, external URLs or image files. Authors should preview the actual shape; validation restricts structure and characters rather than judging visual quality.

The repository preserves these fields and shows categories and icons in public lists, search and detail pages; search includes category names. The Designer groups icons into collapsible categories from these declarations, with hover details. Every operator provides its own icon, which may differ from others in its category. Built-in `read_item` also explicitly declares its category and icon.

Publication still uses the [authenticated upload API](operator-packaging_en.md) or administrator CLI. Include these fields in `--ports`; no new CLI flags are needed. Full definitions also require execution settings such as `name`, `version` and `image`. Shared validation lives in `dacp.operator.OperatorPresentation` in the lightweight `dacp-operator-contract` module.
