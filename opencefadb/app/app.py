# app.py
import base64
import json
import re

import rdflib
import streamlit as st
import streamlit.components.v1 as components
from SPARQLWrapper import SPARQLWrapper, TURTLE  # pip install SPARQLWrapper
from rdflib import Graph, URIRef, RDF
from rdflib.plugins.stores.sparqlstore import SPARQLStore

st.set_page_config(page_title="RDF Graph Explorer", layout="wide")

st.title("🔬 RDF Graph Explorer")
st.markdown("**Roots → Double-click expand → Hierarchical RDF exploration**")


def find_root_nodes(graph: rdflib.Graph, *, uris_only: bool = True) -> set[URIRef]:
    """Root nodes = subjects with no incoming edges (?x ?p s)."""
    subjects = set()
    objects = set()

    for s, p, o in graph:
        subjects.add(s)
        if isinstance(o, (rdflib.term.URIRef, rdflib.term.BNode)):
            objects.add(o)

    candidates = subjects - objects
    if uris_only:
        candidates = {n for n in candidates if isinstance(n, URIRef)}
    return candidates


def get_root_node_iris(graph: rdflib.Graph) -> list[URIRef]:
    roots = find_root_nodes(graph, uris_only=True)
    return sorted(roots)


def get_root_node_iri(graph: rdflib.Graph) -> URIRef | None:
    roots = get_root_node_iris(graph)
    return roots[0] if roots else None


# ---------- SPARQL / Remote loading helpers ----------

def _basic_auth_header(username: str, password: str) -> str:
    token = base64.b64encode(f"{username}:{password}".encode("utf-8")).decode("ascii")
    return f"Basic {token}"


def load_graph_from_rdflib_endpoint(endpoint_url: str, limit: int = 5000) -> Graph:
    sparql = SPARQLWrapper(endpoint_url)
    sparql.setReturnFormat(TURTLE)

    q = f"""
    CONSTRUCT {{ ?s ?p ?o }}
    WHERE      {{ ?s ?p ?o }}
    LIMIT {int(limit)}
    """
    sparql.setQuery(q)

    ttl_bytes = sparql.query().convert()  # bytes (turtle)
    if isinstance(ttl_bytes, str):
        ttl_bytes = ttl_bytes.encode("utf-8")

    g = Graph()
    g.parse(data=ttl_bytes.decode("utf-8"), format="turtle")
    return g


def load_graph_from_sparql_endpoint(
        endpoint_url: str,
        roots: list[str] | None = None,
        limit: int = 5000,
        username: str | None = None,
        password: str | None = None,
) -> Graph:
    """
    Fetch a bounded subgraph from a SPARQL endpoint into a local Graph via CONSTRUCT.
    If roots given: fetch outgoing+incoming around roots.
    Else: fetch first `limit` triples.
    """
    headers = {}
    if username and password:
        headers["Authorization"] = _basic_auth_header(username, password)

    store = SPARQLStore(endpoint_url, headers=headers) if headers else SPARQLStore(endpoint_url)
    remote = Graph(store=store)

    local = Graph()

    if roots:
        # basic IRI wrapping for VALUES
        roots_iris = []
        for r in roots:
            r = r.strip()
            if not r:
                continue
            if r.startswith("<") and r.endswith(">"):
                roots_iris.append(r)
            else:
                roots_iris.append(f"<{r}>")

        if not roots_iris:
            return local

        values = " ".join(roots_iris)

        q = f"""
        CONSTRUCT {{
          ?root ?p ?o .
          ?s ?p2 ?root .
        }}
        WHERE {{
          VALUES ?root {{ {values} }}
          {{
            ?root ?p ?o .
          }} UNION {{
            ?s ?p2 ?root .
          }}
        }}
        LIMIT {int(limit)}
        """
    else:
        q = f"""
        CONSTRUCT {{
          ?s ?p ?o .
        }}
        WHERE {{
          ?s ?p ?o .
        }}
        LIMIT {int(limit)}
        """

    res = remote.query(q)
    if hasattr(res, "graph") and res.graph is not None:
        local += res.graph
    return local


# ---------- HTML / Vis.js ----------

GRAPH_HTML = """
<!DOCTYPE html>
<html>
<head>
  <script src="https://cdn.jsdelivr.net/npm/vis-network/standalone/umd/vis-network.min.js"></script>
  <style>
    body { margin: 0; font-family: Arial; background: #f8f9fa; }
    #graph { position: absolute; top: 0; left: 0; right: 0; bottom: 220px; border: none; }
    .status { position: absolute; top: 10px; left: 10px; background: white; padding: 10px;
              border-radius: 5px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); font-weight: bold; z-index: 200; }
    .toolbar { position:absolute; top:10px; right:10px; z-index:210; display:flex; gap:8px; }
    .toolbar button { padding:4px 8px; font-size:11px; cursor:pointer; border-radius:4px;
                      border:1px solid #ccc; background:#fff; }
    .toolbar button:hover { background:#f0f0f0; }
    #payload {
      position:absolute;
      bottom:10px;
      left:10px;
      right:10px;
      max-height:200px;
      overflow:auto;
      background:#fff;
      border:1px solid #ddd;
      padding:10px;
      z-index:100;
      opacity:0.97;
      font-size:12px;
      color:#222;
    }
    #payload h4 { margin:0 0 4px 0; font-size:13px; }
    #payload .section-title { font-weight:bold; margin-top:6px; margin-bottom:2px; }
    #payload ul { margin:2px 0 4px 16px; padding:0; }
    #payload li { margin-bottom:2px; }
    #payload code { font-size:11px; }
  </style>
</head>
<body>
  <div id="graph"></div>
  <div class="status" id="status">🖕 Double-click roots to expand</div>
  <div class="toolbar">
    <button id="cleanupBtn" title="Remove nodes without any edges">Cleanup</button>
  </div>
  <div id="payload">
    <h4>Node details</h4>
    <div>Click a node to see its details here.</div>
  </div>

  <script>
    const nodes = new vis.DataSet({nodes_json});
    const edges = new vis.DataSet([]);
    const outgoing = {outgoing_json};
    const nodeMeta = {node_meta_json};

    // secondary index by local name -> list of children
    const outgoingByLocal = {};
    Object.keys(outgoing).forEach(function(k){
      try {
        const local = k.split(/[\\/\\#]/).pop();
        outgoingByLocal[local] = outgoingByLocal[local] || [];
        outgoingByLocal[local] = outgoingByLocal[local].concat(outgoing[k]);
      } catch(e) {}
    });

    const container = document.getElementById('graph');
    const data = { nodes, edges };

    const options = {
      physics: { enabled: true, solver: 'forceAtlas2Based', forceAtlas2Based: { springLength: 150 } },
      nodes: { shape: 'dot', size: 20, font: { size: 11 } },
      edges: { arrows: 'to', font: { size: 9 } }
    };

    let network;
    let expanded = new Set();

    function shortName(uri) {
      return String(uri).split('/').pop().split('#').pop();
    }

    function escapeHtml(str) {
      return String(str)
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;");
    }

    function showNodeDetails(id) {
      try {
        const n = nodes.get(id);
        const meta = nodeMeta[id] || {};
        const out = outgoing[id] || [];
        const payloadEl = document.getElementById('payload');
        if (!payloadEl) return;

        const label = n ? n.label : shortName(id);
        const iri = n ? n.title : String(id);

        const metaKeys = Object.keys(meta);
        let metaHtml = '';
        if (metaKeys.length === 0) {
          metaHtml = '<em>No literal properties available.</em>';
        } else {
          metaHtml = '<ul>' + metaKeys.map(k => {
            const values = meta[k] || [];
            const valuesText = values.map(v => escapeHtml(v)).join(', ');
            return `<li><strong>${escapeHtml(k)}</strong>: ${valuesText}</li>`;
          }).join('') + '</ul>';
        }

        let outHtml = '';
        if (!out.length) {
          outHtml = '<em>No outgoing links from this node.</em>';
        } else {
          outHtml = '<ul>' + out.map(child => {
            const pred = child.label || '';
            const targetShort = shortName(child.to);
            return `<li><strong>${escapeHtml(pred)}</strong> → ${escapeHtml(targetShort)}</li>`;
          }).join('') + '</ul>';
        }

        const html = `
          <h4>Node details</h4>
          <div style="margin-bottom:6px;">
            <div><strong>Label:</strong> ${escapeHtml(label)}</div>
            <div><strong>IRI:</strong>
              <code>
                <a href="${iri}" target="_blank" rel="noopener noreferrer">
                  ${escapeHtml(iri)}
                </a>
              </code>
            </div>
          </div>
          <div style="margin-bottom:6px;">
            <div class="section-title">Properties</div>
            ${metaHtml}
          </div>
          <div>
            <div class="section-title">Links</div>
            ${outHtml}
          </div>
        `;
        payloadEl.innerHTML = html;
      } catch (e) {
        console.log('showNodeDetails error', e);
      }
    }

    function expandNode(id) {
      expanded.add(id);
      const idLocal = String(id).split(/[\\/\\#]/).pop();
      const children = outgoing[id] || outgoingByLocal[idLocal] || [];
      let added = 0;

      children.forEach(child => {
        try {
          if (!nodes.get(child.to)) {
            let pos = null;
            try { pos = network.getPositions([id])[id]; } catch(e) { pos = null; }
            const nodeObj = {
              id: child.to,
              label: shortName(child.to),
              title: child.to,
              color: { background: '#3498db' }
            };
            if (pos) {
              const nx = pos.x + (Math.random() - 0.5) * 120;
              const ny = pos.y + (Math.random() - 0.5) * 120;
              nodeObj.x = nx; nodeObj.y = ny;
              nodeObj.fixed = { x: true, y: true };
              nodeObj.physics = false;
            }
            nodes.add(nodeObj);
            if (pos) {
              setTimeout(() => {
                try {
                  nodes.update({ id: child.to, fixed: { x: false, y: false }, physics: true });
                } catch(e) {}
              }, 700);
            }
          }
          const eid = id + '|' + child.to;
          try {
            if (!edges.get(eid)) edges.add({ id: eid, from: id, to: child.to, label: child.label });
          } catch(e) {
            try { edges.add({ id: eid, from: id, to: child.to, label: child.label }); } catch(e) {}
          }
          added++;
        } catch(err) {
          console.log('expandNode error', err);
        }
      });

      const st = document.getElementById('status');
      if (st) st.innerHTML = `🔓 Expanded ${shortName(id)} (+${added} children)`;
      network.fit();
      showNodeDetails(id);
    }

    function collapseNode(id) {
      expanded.delete(id);
      const children = outgoing[id] || [];
      let removed = 0;

      children.forEach(child => {
        try {
          if (nodes.get(child.to)) {
            nodes.remove(child.to);
            removed++;
          }
          const eid = id + '|' + child.to;
          if (edges.get(eid)) edges.remove(eid);
        } catch(err) {
          console.log('collapseNode error', err);
        }
      });

      const st = document.getElementById('status');
      if (st) st.innerHTML = `🔒 Collapsed ${shortName(id)} (-${removed} children)`;
      network.fit();
      showNodeDetails(id);
    }

    function cleanupIsolatedNodes() {
      try {
        const allNodeIds = nodes.getIds ? nodes.getIds() : nodes.get().map(n => n.id);
        const allEdges = edges.get();
        const connected = new Set();

        allEdges.forEach(e => {
          if (e.from != null) connected.add(e.from);
          if (e.to != null) connected.add(e.to);
        });

        const toRemove = [];
        allNodeIds.forEach(id => {
          if (!connected.has(id)) toRemove.push(id);
        });

        if (toRemove.length === 0) {
          const st = document.getElementById('status');
          if (st) st.innerHTML = '✅ Cleanup: no isolated nodes to remove';
          return;
        }

        nodes.remove(toRemove);

        const st = document.getElementById('status');
        if (st) st.innerHTML = `🧹 Cleanup: removed ${toRemove.length} isolated node(s)`;

        const payloadEl = document.getElementById('payload');
        if (payloadEl) {
          payloadEl.innerHTML = '<h4>Node details</h4><div>Click a node to see its details here.</div>';
        }
      } catch (e) {
        console.log('cleanupIsolatedNodes error', e);
      }
    }

    try {
      network = new vis.Network(container, data, options);

      // Auto-expand first root and show details
      setTimeout(() => {
        try {
          const rootIds = nodes.getIds ? nodes.getIds() : nodes.get().map(n=>n.id);
          if (rootIds && rootIds.length > 0) {
            expandNode(rootIds[0]);
            showNodeDetails(rootIds[0]);
          }
        } catch(e) { console.log('auto-expand error', e); }
      }, 400);

    } catch (e) {
      console.error('Failed to create vis.Network', e);
      const stEl = document.getElementById('status');
      if (stEl) stEl.innerText = 'Error creating network: ' + e;
    }

    // Handlers
    try {
      if (network) {
        network.on('doubleClick', ({ nodes: ids }) => {
          const id = ids && ids[0];
          if (!id) return;
          if (expanded.has(id)) collapseNode(id); else expandNode(id);
        });

        network.on('hoverNode', ({ node }) => {
          const nodeData = nodes.get(node);
          const st = document.getElementById('status');
          if (st) st.innerText = `Hover: ${nodeData ? nodeData.label : node}`;
        });

        network.on('click', ({ nodes: ids }) => {
          const id = ids && ids[0];
          if (!id) return;
          showNodeDetails(id);
        });
      }
    } catch(e) { console.log('attach handlers error', e); }

    // Toolbar
    try {
      const cleanupBtn = document.getElementById('cleanupBtn');
      if (cleanupBtn) cleanupBtn.addEventListener('click', cleanupIsolatedNodes);
    } catch (e) { console.log('toolbar binding error', e); }
  </script>
</body>
</html>
"""

# ---------- Sidebar: Source selection ----------
st.sidebar.header("RDF data source")

source_mode = st.sidebar.radio(
    "Choose source",
    ["Upload file", "SPARQL endpoint (GraphDB etc.)", "Remote RDF URL"],
    index=0,
)

uploaded_file = None
sparql_endpoint_url = ""
remote_rdf_url = ""

sparql_limit = 5000
sparql_user = ""
sparql_pass = ""

if source_mode == "Upload file":
    uploaded_file = st.sidebar.file_uploader(
        "Choose an RDF file",
        type=["ttl", "nt", "rdf", "xml", "jsonld", "n3"],
    )

elif source_mode == "SPARQL endpoint (GraphDB etc.)":
    st.sidebar.caption("GraphDB example: https://HOST/repositories/REPO")
    sparql_endpoint_url = st.sidebar.text_input("SPARQL endpoint URL", value="")
    sparql_limit = st.sidebar.number_input(
        "Max triples to fetch (CONSTRUCT LIMIT)",
        min_value=100,
        max_value=200000,
        value=5000,
        step=100,
    )
    with st.sidebar.expander("Auth (optional)"):
        sparql_user = st.text_input("Username", value="")
        sparql_pass = st.text_input("Password", value="", type="password")

elif source_mode == "Remote RDF URL":
    st.sidebar.caption("Any RDF document URL rdflib can parse (TTL/RDFXML/JSON-LD/etc.)")
    remote_rdf_url = st.sidebar.text_input("RDF document URL", value="")
    sparql_limit = st.sidebar.number_input(
        "Max triples to fetch (CONSTRUCT LIMIT)",
        min_value=100,
        max_value=200000,
        value=5000,
        step=100,
    )

# Root selection (shared for all modes; especially important for SPARQL)
st.sidebar.header("Root IRI (optional)")
root_input = st.sidebar.text_area(
    "Root IRI(s) — comma or newline separated",
    value="",
    help="If using a SPARQL endpoint, provide roots to fetch a small subgraph around them.",
)

root_iris_from_input: list[str] = []
if root_input and root_input.strip():
    root_iris_from_input = [p.strip() for p in re.split(r"[,\n\r]+", root_input) if p.strip()]


# helper: local name using namespace manager
def local_name(uri, graph):
    try:
        prefix, namespace, lname = graph.namespace_manager.compute_qname(URIRef(uri))
        return lname
    except Exception:
        return str(uri).split("/")[-1].split("#")[-1]


# ---------- Main: Graph Visualization ----------
st.subheader("Graph Visualization")

g: Graph | None = None
ttl_content: str | None = None  # only for debug display when uploading

if source_mode == "Upload file":
    if uploaded_file is None:
        st.info("Upload an RDF file to visualize it.")
        st.stop()

    ttl_bytes = uploaded_file.read()
    try:
        ttl_content = ttl_bytes.decode("utf-8")
    except Exception:
        try:
            ttl_content = ttl_bytes.decode("latin-1")
        except Exception:
            ttl_content = ttl_bytes.decode("utf-8", errors="ignore")

    with st.spinner("🔄 Parsing uploaded RDF with RDFLib..."):
        g = Graph()
        try:
            g.parse(data=ttl_content, format="turtle")
        except Exception:
            # let rdflib guess
            g.parse(data=ttl_content)

elif source_mode == "Remote RDF URL":
    if not remote_rdf_url.strip():
        st.info("Enter an RDF document URL to visualize it.")
        st.stop()
    with st.spinner("🌐 Fetching + parsing remote RDF URL..."):
        g = load_graph_from_rdflib_endpoint(remote_rdf_url.strip(), limit=sparql_limit)

elif source_mode == "SPARQL endpoint (GraphDB etc.)":
    if not sparql_endpoint_url.strip():
        st.info("Enter a SPARQL endpoint URL to visualize it.")
        st.stop()
    with st.spinner("🔌 Fetching subgraph from SPARQL endpoint (CONSTRUCT)..."):
        g = load_graph_from_sparql_endpoint(
            endpoint_url=sparql_endpoint_url.strip(),
            roots=root_iris_from_input if root_iris_from_input else None,
            limit=int(sparql_limit),
            username=sparql_user.strip() or None,
            password=sparql_pass or None,
        )

if g is None:
    st.error("Graph could not be loaded.")
    st.stop()

if len(g) == 0:
    st.warning("Loaded graph has 0 triples. Try increasing the LIMIT or providing Root IRIs (for SPARQL).")
    st.stop()

# Build nodes and outgoing mapping
all_nodes: dict[str, dict] = {}
outgoing_triples: dict[str, list[dict]] = {}
incoming_triples: dict[str, list[dict]] = {}

for s, p, o in g:
    s_str, p_str, o_str = str(s), str(p), str(o)

    if s_str not in all_nodes:
        label = local_name(s_str, g)
        is_class = bool(list(g.objects(URIRef(s_str), RDF.type)))
        all_nodes[s_str] = {
            "id": s_str,
            "label": label[:40],
            "title": s_str,
            "color": "#e74c3c" if is_class else "#3498db",
            "meta": {},
        }

    if isinstance(o, URIRef):
        if o_str not in all_nodes:
            o_label = local_name(o_str, g)
            all_nodes[o_str] = {
                "id": o_str,
                "label": o_label[:40],
                "title": o_str,
                "color": "#3498db",
                "meta": {},
            }

        edge_label = local_name(p_str, g)[:24]
        outgoing_triples.setdefault(s_str, []).append({"to": o_str, "label": edge_label})

        incoming_triples.setdefault(o_str, []).append({"from": s_str, "label": edge_label})

        try:
            if URIRef(p_str) == RDF.type:
                outgoing_triples.setdefault(o_str, []).append({"to": s_str, "label": "instance"})
        except Exception:
            pass

    else:
        try:
            pred_name = local_name(p_str, g)[:40]
            meta = all_nodes[s_str].setdefault("meta", {})
            meta.setdefault(pred_name, []).append(o_str)
        except Exception:
            pass

# Determine initial nodes shown:
# - If user entered root IRIs: show those that exist in loaded graph.
# - Else: show up to 20 nodes.
roots: list[str] = []
if root_iris_from_input:
    matched = [r for r in root_iris_from_input if r in all_nodes]
    if matched:
        roots = matched
    else:
        st.warning("None of the provided Root IRIs were found in the loaded subgraph. Showing first nodes instead.")
        roots = list(all_nodes.keys())[:20]
else:
    roots = list(all_nodes.keys())[:20]

initial_nodes: list[dict] = []
for root in roots[:12]:
    initial_nodes.append(all_nodes[root].copy())

nodes_json = json.dumps(initial_nodes)
outgoing_json = json.dumps(outgoing_triples)
node_meta = {k: v.get("meta", {}) for k, v in all_nodes.items()}
node_meta_json = json.dumps(node_meta)

if not initial_nodes:
    st.warning("No nodes found — check parsing / endpoint query.")
    if ttl_content:
        st.markdown("**TTL (first 2000 chars)**")
        st.code(ttl_content[:2000])
    st.stop()

# Render Vis.js graph
html = GRAPH_HTML.replace("{nodes_json}", nodes_json)
html = html.replace("{outgoing_json}", outgoing_json)
html = html.replace("{node_meta_json}", node_meta_json)

# NOTE: If you couldn't see the plot before, it was often because
# components.html wasn't called (or g was empty / st.stop before render).
components.html(html, height=750, scrolling=False)

# Summary
st.subheader("Graph summary")
st.write(
    f"Triples: {len(g)} — Nodes: {len(all_nodes)} — Roots shown: {len(initial_nodes)} — Outgoing entries: {len(outgoing_triples)}"
)

st.markdown("---")
st.caption("🐍 RDFLib + Vis.js | `pip install streamlit rdflib` | 🔬 Hierarchical RDF exploration")
