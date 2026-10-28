# Operator parameter contracts

Every operator must provide `parameters` in its `operator.yaml` definition and published `manifest.json`. This language-neutral contract can be read statically for Python, Java/Scala, native commands, and custom executors. Reading it starts neither an operator nor an inference worker; types are not guessed from descriptions or example values.

Input streams are declared in `inputs`, parameters in `parameters`, and output structures by `inferOutputs` / `infer_outputs`. These are independent contracts. A parameter contract is not an output schema.

## Complete example

```yaml
name: weather_filter
version: "1.0"
image: example/python:1
executor: python
entrypoint: weather_filter
artifact: weather_filter.py
inputs:
  source: {representations: [records], formats: []}
category: Data transformation
icon:
  path: "M3 5h18v14H3zM3 10h18M9 5v14"
  color: "#14866b"
parameters:
  type: object
  properties:
    min_temperature:
      type: number
      description: Minimum retained temperature in degrees Celsius.
      default: 0
      minimum: -100
      maximum: 100
    station:
      type: string
      description: Station name to retain.
      minLength: 1
    include_quality:
      type: boolean
      description: Include the quality flag in output.
      default: true
    columns:
      type: array
      description: Output column names in the requested order.
      items: {type: string, minLength: 1}
      minItems: 1
  required: [station]
  additionalProperties: false
```

A Job step supplies values, rather than the contract:

```yaml
params:
  station: DEMO-001
  min_temperature: $MIN_TEMPERATURE
  include_quality: true
  columns: [timestamp, temperature]
```

Variable file:

```yaml
MIN_TEMPERATURE: 12.5
```

The declared type remains `number`, not `variable`. `$MIN_TEMPERATURE` is a whole-value reference, and substitution preserves YAML types. Python reads a number through `ctx.params['min_temperature']`; Scala reads it through `ctx.params.path("min_temperature").asDouble()`.

## Supported declarations

The contract uses a restricted JSON Schema-style subset; it does not claim full JSON Schema support. Unknown keywords are rejected so that editors do not silently ignore constraints.

| Keyword | Purpose |
|---|---|
| `type` | Required: `string`, `integer`, `number`, `boolean`, `null`, `object`, or `array`; also accepts a nonempty array of distinct types, such as `[string, object]` |
| `title`, `description` | Optional display name and explanation |
| `default` | Optional default value satisfying the declared type and constraints |
| `enum` | Optional nonempty array of distinct values, each satisfying the type and constraints |
| `minimum`, `maximum` | Inclusive numeric bounds |
| `minLength`, `maxLength` | String length bounds measured in Unicode code points |
| `items`, `minItems`, `maxItems` | Arrays must declare their element definition and may set length bounds |
| `properties` | Required property mapping for objects, recursively using the same contract |
| `required` | Required array of mandatory property names for objects; use `[]` when none are required |
| `additionalProperties` | Explicitly required for objects: `false` rejects undeclared properties, `true` permits dynamic properties, or a nested definition restricts their value types |

The root must have `type: object`. Nesting is limited to 16 levels. Operators without parameters must explicitly declare:

```yaml
category: Data transformation
icon:
  path: "M3 5h18v14H3zM3 10h18M9 5v14"
  color: "#14866b"
parameters:
  type: object
  properties: {}
  required: []
  additionalProperties: false
```

A dynamic mapping, such as column names mapped to aggregate functions:

```yaml
category: Data transformation
icon:
  path: "M3 5h18v14H3zM3 10h18M9 5v14"
  color: "#14866b"
parameters:
  type: object
  properties:
    aggregates:
      type: object
      properties: {}
      required: []
      additionalProperties: {type: string, enum: [AVG, SUM, MIN, MAX, COUNT]}
  required: [aggregates]
  additionalProperties: false
```

## Publication, design, and execution

- Publication and seeding validate structure, defaults, enum values, bounds, and required property declarations. Definitions without `parameters` cannot be published. The file supplied to `publish --ports FILE` must contain both `inputs` and `parameters`; a complete `operator.yaml` can be supplied directly. HTTP manifest uploads follow the same rules.
- Repository list and detail APIs return complete definitions, and the detail webpage displays Parameters. Manifest version remains `api_version: 2`; this unreleased project does not retain compatibility branches for superseded definitions.
- On opening, the designer automatically connects to the configured Union (preferred) or node and independently loads the default repository. Service configuration selects connections; the interface provides no connection switcher. The inspector has **Step**, **Parameters**, and **Outputs** tabs.
- Parameters generates fields from declarations. The operator determines names and permitted types; the form shows required status, descriptions, defaults, enums, and bounds. Enable optional parameters using checkboxes; disabled parameters are omitted from the Job. Defaults appear as hints and initial editing values, and are not injected by the server.
- Valid edits save automatically and synchronize Job YAML. Incomplete or invalid input stays in the form with a message. Variable references are resolved on submission. Without the corresponding repository definition, the editor asks for it to be loaded instead of treating manually selected types as a formal contract. Browser numbers use JavaScript Number; integer inputs are restricted to its safe integer range.
- This adds no pre-execution `inferSchema` or parameter inference call and no runtime default injection. Operator implementations remain responsible for business validation and omitted-value behavior. Authors must keep declarations consistent with implementations.

The shared Scala validator lives in `dacp-operator-contract`. The repository, node resolver, and designer use the same implementation, without operator-name or executor-specific branches. Third-party definitions are discoverable without editing core registries.

Operators must also provide `category` and `icon`; see [categories and icons](operator-presentation_en.md) for the contract and publication examples.
