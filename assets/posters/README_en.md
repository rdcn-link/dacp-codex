# DACP meeting poster

The English meeting poster introduces autonomous data nodes, bytes / Arrow records streams, Java / Scala, Python and native command operators, and Union workflows and execution monitoring across nodes.

- [PNG poster](dacp-meeting-poster-en.png): for previews and digital sharing.
- [A4 PDF](dacp-meeting-poster-en-a4.pdf): one A4 page preserving the PNG aspect ratio, with a clickable repository address in the footer.
- [Generation prompt](dacp-meeting-poster-en-prompt.txt): records copy, layout and diagram constraints.

The poster uses the [DACP logo](../dacp-logo.png), retaining the C-shaped symbol and its purple, teal and amber nodes. Dashed lines show Union coordination with servers A, B and C; solid lines show direct A → B and C → B data streams. Union coordinates collaborative workflows; each server can independently serve data.

imagegen updated the PNG using the previous poster and existing logo. The PDF embeds the same PNG without further image edits. The project remains an MVP; the poster makes no performance claims or guarantees such as “data never moves”.
