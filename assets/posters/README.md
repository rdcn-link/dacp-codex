# DACP meeting poster

The English meeting poster introduces autonomous data nodes, record and byte streams, YAML streaming jobs, reusable operators, execution policies and operational monitoring. It presents user capabilities without listing low-level transport technologies.

- [PNG poster](dacp-meeting-poster-en.png): for previews and digital sharing.
- [A4 PDF](dacp-meeting-poster-en-a4.pdf): one A4 page preserving the PNG aspect ratio, with a clickable repository address in the footer.
- [Generation prompt](dacp-meeting-poster-en-prompt.txt): records the full copy, layout and diagram constraints.

The poster retains the [DACP logo](../dacp-logo.png) and its purple, teal and amber nodes. Dashed lines show Union coordination with A, B and C. Solid lines show direct A → B → C data streams; final results return from C through Union to the client. Each node can also provide independent data access.

Content covers pluggable catalogs, Java / Scala, Python and native command operators, variables, timeouts, retries, optional materialization and replay, and monitoring of DAGs, health and transfer rates. Execution policies control materialization; intermediate results are not saved for every job.

The built-in imagegen tool updated the PNG using the previous poster and existing logo; the PDF embeds the same PNG. The project remains an MVP, with no performance figures or guarantees such as “data never moves”.
