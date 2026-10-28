# DACP official website

An independent Scala HTTP service with English-only website content. It provides a project overview, user / administrator / developer documentation, and server and client downloads. It uses the project logo and C favicon without external fonts, scripts or CDNs.

## Build and start

Requires JDK 17 and sbt 1.10.x. Run sbt inside the module directory:

```bash
cd dacp-website
sbt test stage
target/universal/stage/bin/dacp-website --host 0.0.0.0 --port 8830 --downloads /srv/dacp/downloads
```

Open `http://localhost:8830/`. `/docs` provides documentation navigation; `/downloads` lists the supported software packages present in the download directory; paths such as `/guide/docs/users/README.md` render documentation. Defaults are `0.0.0.0:8830` and a `downloads` directory relative to the process working directory.

The website does not handle data access, task execution, member scheduling or operator publication. For public deployment, configure a domain and HTTPS at a reverse proxy; allow only administrators to write the download directory. All pages and downloads are public.

## Prepare software downloads

From the repository root, run the following command to build components sequentially and generate checksums:

```bash
python3 scripts/prepare-website-downloads.py --output /srv/dacp/downloads
```

The script requires sbt, Python 3.10+, pip and network access. It builds only the `dacp-server` and `dacp-union-server` ZIP distributions and the `dacp-client` Python wheel, including `dacp-shell`. It generates `SHA256SUMS` for verification. Artifacts are development snapshots; pip installs client dependencies. The download page lists only these three software components, using 1024-byte units: MB with one decimal place (such as `134.9MB`), whole kB (such as `28kB`), and bytes for files below 1 kB. Website, source, Scala SDK and collaboration SDK bundles are not published in the download catalog.

Use `--skip-build` to reuse server ZIPs; the client wheel is always regenerated. Missing server distributions fail the operation. Files are replaced atomically one by one and discovered without a website restart. The directory update is not transactional as a whole; verify checksums after publication completes. Existing unrelated files are not deleted, but cannot be listed or downloaded through the website.

## Maintain documentation

The website build collects repository `*.md` files, excluding Chinese `*_zh.md` files, hidden paths and build directories. Documentation ships inside the website distribution and does not depend on a source checkout at runtime. Role guides, the protocol, discovery, streams, operators, plugins, deployment, access logs and execution history can all be read online. Markdown tables are supported and raw HTML is escaped; local documentation links remain on-site, while other source / configuration links point to GitHub. Rebuild and redeploy the website after updating documentation.

```bash
cd dacp-website
sbt test Universal/packageBin
```

Tests cover the main pages, packaged documentation, download contents and filenames, path traversal, symbolic links, missing resources and favicon.
