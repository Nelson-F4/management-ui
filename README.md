# management-ui

Static operator UI for Nelson4 (HTML, CSS, JavaScript). It is a **Python package** so `control-plane` can depend on it and mount assets at `/` and `/assets`.

**Repository:** [github.com/Nelson-F4/management-ui](https://github.com/Nelson-F4/management-ui) (same GitHub org as `control-plane`, `execution-runtime`, and the other Nelson packages).

## Install

From the Nelson4 repo root (sibling of `control-plane`):

```bash
pip install -e ./management-ui
pip install -e ./control-plane
```

Docker builds install `management-ui` before `control-plane` (see `docker/control-plane.Dockerfile` in the Nelson4 repo).

Install from Git without a local clone:

```bash
pip install "management-ui @ git+https://github.com/Nelson-F4/management-ui.git"
```

## Contents

- `src/management_ui/static/` — `index.html`, `app.js`, `styles.css`, `favicon.svg`

The UI expects the **control-plane** API at the same origin (`/api/v1/...`), compatible with the Nelson3 management dashboard contract.

## Versioning

Bump the `?v=` query on `styles.css` and `app.js` in `index.html` together when you change those assets (cache bust).
