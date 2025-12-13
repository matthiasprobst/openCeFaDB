import json

import streamlit as st
import streamlit.components.v1 as components
from rdflib import Graph, URIRef, RDF
import rdflib
st.set_page_config(page_title="RDF Graph Explorer", layout="wide")

st.title("🔬 RDF Graph Explorer")
st.markdown("**Roots → Double-click expand → Hierarchical RDF exploration**")



def find_root_nodes(graph: rdflib.Graph, *, uris_only: bool = True) -> set[URIRef]:
    """Bestimme Wurzelknoten des RDF-Graphen.

    Wurzelknoten sind hier definiert als Subjekte, für die es im Graphen
    keine eingehenden Kanten (also keine Triple ?x ?p s) gibt.

    Parameter
    ---------
    graph : rdflib.Graph
        Der zu untersuchende Graph.
    uris_only : bool, optional
        Wenn True, werden nur URIRefs (keine Blank Nodes / Literals) als
        Wurzeln zurückgegeben. Default: True.

    Returns
    -------
    set[URIRef]
        Menge der gefundenen Wurzelknoten (als URIRef).
    """

    subjects = set()
    objects = set()

    for s, p, o in graph:
        subjects.add(s)
        # Nur Ressourcen (URI oder BNode) als potentielle Objekt-Knoten
        if isinstance(o, (rdflib.term.URIRef, rdflib.term.BNode)):
            objects.add(o)

    candidates = subjects - objects

    if uris_only:
        candidates = {n for n in candidates if isinstance(n, URIRef)}

    return candidates


def get_root_node_iris(graph: rdflib.Graph) -> list[URIRef]:
    """Gib alle Root-IRIs für den gegebenen Graphen zurück (sortiert).

    Gibt eine leere Liste zurück, wenn keine Root-IRI gefunden wurde.
    """

    roots = find_root_nodes(graph, uris_only=True)
    return sorted(roots)


def get_root_node_iri(graph: rdflib.Graph) -> URIRef | None:
    """Gib eine (heuristisch gewählte) Root-IRI für den gegebenen Graphen zurück.

    Falls mehrere Root-Knoten existieren, wird deterministisch der
    lexikografisch kleinste URIRef zurückgegeben.
    Gibt None zurück, wenn keine Root-IRI gefunden wurde.
    """

    roots = get_root_node_iris(graph)
    if not roots:
        return None

    return roots[0]

from rdflib import Graph, URIRef, BNode, Literal
from rdflib.namespace import OWL

def add_inverse_relations(data_ttl: str, ontology: str) -> str:
    """
    Given:
      - data_ttl: TTL string with instance data
      - ontology_ttl: TTL string that may contain owl:inverseOf definitions
    Returns:
      - new TTL string where all inverse relations have been added
    """
    # Parse instance data and ontology
    g = Graph()
    g.parse(data=data_ttl, format="turtle")

    ont = Graph()
    if str(ontology).startswith("http"):
        ont.parse(source=ontology)
    else:
        ont.parse(data=ontology, format="turtle")

    # Build map: predicate -> set of inverse predicates
    inverse_map = {}

    for p, _, q in ont.triples((None, OWL.inverseOf, None)):
        # p owl:inverseOf q  => p inverse q, q inverse p
        inverse_map.setdefault(p, set()).add(q)
        inverse_map.setdefault(q, set()).add(p)

    # Add inverse triples
    new_triples = []
    for s, p, o in g:
        # Only add inverse for non-literal objects
        if isinstance(o, Literal):
            continue
        if p in inverse_map:
            for inv_pred in inverse_map[p]:
                inv_triple = (o, inv_pred, s)
                if inv_triple not in g:
                    new_triples.append(inv_triple)

    # Actually add them to the graph
    for (s, p, o) in new_triples:
        g.add((s, p, o))

    # Return TTL string
    return g.serialize(format="turtle")


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
        .toolbar button { padding:4px 8px; font-size:11px; cursor:pointer; border-radius:4px; border:1px solid #ccc; background:#fff; }
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

        // build a secondary index by local name (last segment after / or #) -> list of children
        const outgoingByLocal = {};
        Object.keys(outgoing).forEach(function(k){
            try {
                const local = k.split(/[\\/\\#]/).pop();
                outgoingByLocal[local] = outgoingByLocal[local] || [];
                outgoingByLocal[local] = outgoingByLocal[local].concat(outgoing[k]);
            } catch(e) { /* ignore */ }
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

                // Properties (literals)
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

                // Outgoing links
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
                    if (!connected.has(id)) {
                        toRemove.push(id);
                    }
                });

                if (toRemove.length === 0) {
                    const st = document.getElementById('status');
                    if (st) st.innerHTML = '✅ Cleanup: no isolated nodes to remove';
                    return;
                }

                nodes.remove(toRemove);

                const st = document.getElementById('status');
                if (st) st.innerHTML = `🧹 Cleanup: removed ${toRemove.length} isolated node(s)`;

                // if current selected node was removed, clear details
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
            console.log('vis.Network created');

            // Auto-expand first root and show its details
            try {
                setTimeout(() => {
                    try {
                        const rootIds = nodes.getIds ? nodes.getIds() : nodes.get().map(n=>n.id);
                        if (rootIds && rootIds.length > 0) {
                            console.log('Auto-expanding first root', rootIds[0]);
                            expandNode(rootIds[0]);
                            showNodeDetails(rootIds[0]);
                        }
                    } catch(e) { console.log('auto-expand error', e); }
                }, 400);
            } catch(e) { console.log('auto-expand scheduling error', e); }
        } catch (e) {
            console.error('Failed to create vis.Network', e);
            const stEl = document.getElementById('status');
            if (stEl) stEl.innerText = 'Error creating network: ' + e;
        }

        // Diagnostic: show counts in status
        try {
            const nCount = nodes.getIds ? nodes.getIds().length : 0;
            const oCount = Object.keys(outgoing).length;
            console.log('nodes count', nCount, 'outgoing count', oCount);
            const stEl2 = document.getElementById('status');
            if (stEl2) stEl2.innerText += ` | nodes:${nCount} outgoing:${oCount}`;
        } catch(e) { console.log('diag error', e); }

        // Attach event handlers
        try {
            if (typeof network !== 'undefined' && network) {
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
            } else {
                console.log('Network not available; handlers not attached');
            }
        } catch(e) { console.log('attach handlers error', e); }

        // Toolbar button events
        try {
            const cleanupBtn = document.getElementById('cleanupBtn');
            if (cleanupBtn) {
                cleanupBtn.addEventListener('click', () => {
                    cleanupIsolatedNodes();
                });
            }
        } catch (e) {
            console.log('toolbar binding error', e);
        }
     </script>
</body>
</html>
"""

# Sidebar for graph data upload
st.sidebar.header("Upload RDF Data")
uploaded_file = st.sidebar.file_uploader("Choose an RDF file", type=["ttl", "nt", "rdf", "xml"])


# helper: get local name using the graph's namespace manager
def local_name(uri, graph):
    try:
        prefix, namespace, lname = graph.namespace_manager.compute_qname(URIRef(uri))
        return lname
    except Exception:
        return str(uri).split('/')[-1].split('#')[-1]


# Graph visualization
st.subheader("Graph Visualization")
if uploaded_file is not None:
    ttl_bytes = uploaded_file.read()
    # try to detect encoding and decode
    try:
        ttl_content = ttl_bytes.decode("utf-8")
    except Exception:
        try:
            ttl_content = ttl_bytes.decode("latin-1")
        except Exception:
            ttl_content = ttl_bytes.decode("utf-8", errors="ignore")

    # ttl_content = add_inverse_relations(ttl_content, "https://matthiasprobst.github.io/ssno/ssno.ttl")

    with st.spinner("🔄 Parsing with RDFLib..."):
        g = Graph()
        try:
            g.parse(data=ttl_content, format="turtle")
        except Exception as e:
            try:
                g.parse(data=ttl_content)
            except Exception as e2:
                st.error(f"Error parsing RDF: {e} / {e2}")
                raise

    # Build nodes and outgoing mapping
    all_nodes: dict[str, dict] = {}
    outgoing_triples: dict[str, list[dict]] = {}
    # zusätzlich: eingehende Kanten sammeln, um später einfache Inferenz zu machen
    incoming_triples: dict[str, list[dict]] = {}

    for s, p, o in g:
        s_str, p_str, o_str = str(s), str(p), str(o)

        # ensure subject node
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

        # object is IRI: ensure node and add outgoing
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
            outgoing_triples.setdefault(s_str, []).append(
                {"to": o_str, "label": edge_label}
            )

            # einfache Record der eingehenden Relation
            incoming_triples.setdefault(o_str, []).append(
                {"from": s_str, "label": edge_label}
            )

            # if predicate is rdf:type, add reverse mapping class -> instance
            try:
                if URIRef(p_str) == RDF.type:
                    outgoing_triples.setdefault(o_str, []).append(
                        {"to": s_str, "label": "instance"}
                    )
            except Exception:
                pass

        else:
            # o ist Literal: als Property im Node-Meta ablegen
            try:
                pred_name = local_name(p_str, g)[:40]
                meta = all_nodes[s_str].setdefault("meta", {})
                meta.setdefault(pred_name, []).append(o_str)
            except Exception:
                pass

    # --- Einfache Inferenz / Reasoning-Schicht ---------------------------------
    # Ziel: Wenn eine Ressource X auf eine StandardNameTable T zeigt und
    # an anderer Stelle ein StandardName S auf diese Tabelle T zeigt,
    # dann beim Expand von T auch S als Kind sehen.
    #
    # Annahmen/Heuristik:
    # - StandardNameTable- und StandardName-Ressourcen können über RDF-Typen
    #   oder Namenskonventionen (IRI enthält "StandardNameTable" bzw. "StandardName") erkannt werden.

    STANDARD_NAME_TABLE_MARKERS = ("StandardNameTable", "standard_name_table")
    STANDARD_NAME_MARKERS = ("StandardName", "standard_name")

    def is_standard_name_table(uri: str) -> bool:
        lname = local_name(uri, g)
        txt = (uri + " " + lname).lower()
        return any(m.lower() in txt for m in STANDARD_NAME_TABLE_MARKERS)

    def is_standard_name(uri: str) -> bool:
        lname = local_name(uri, g)
        txt = (uri + " " + lname).lower()
        return any(m.lower() in txt for m in STANDARD_NAME_MARKERS)

    # # Baue für jede Tabelle die Menge der StandardNames, die auf sie zeigen.
    # table_to_standard_names: dict[str, set[str]] = {}
    # for target_iri, incomings in incoming_triples.items():
    #     if not is_standard_name_table(target_iri):
    #         continue
    #     for inc in incomings:
    #         src = inc["from"]
    #         if is_standard_name(src):
    #             table_to_standard_names.setdefault(target_iri, set()).add(src)
    #
    # # Füge für jede Tabelle eine synthetische Kante Tabelle -> StandardName hinzu,
    # # sofern diese Richtung noch nicht existiert.
    # for table_iri, std_names in table_to_standard_names.items():
    #     existing = outgoing_triples.get(table_iri, [])
    #     existing_targets = {edge["to"] for edge in existing}
    #     for std in std_names:
    #         if std in existing_targets:
    #             continue
    #         outgoing_triples.setdefault(table_iri, []).append(
    #             {"to": std, "label": "inferred-standardName"}
    #         )

    # Decide initial nodes to show: show roots (nodes without incoming edges) or all classes
    # roots = [str(n) for n in get_root_node_iris(g)]
    # roots = ["https://doi.org/10.5281/zenodo.410121#2023-11-07-15-20-03_run.hdf"]
    # Sidebar input: allow the user to provide one or more root IRIs (comma- or newline-separated).
    st.sidebar.header("Root IRI (optional)")
    root_input = st.sidebar.text_area(
        "Root IRI(s) — comma or newline separated",
        value="",
        help="Gib eine oder mehrere Root-IRIs ein (z.B. https://example.org/node). Wenn leer, werden Standard-Wurzeln verwendet."
    )

    # Parse user input into a list of IRIs
    import re
    roots = []
    if root_input and root_input.strip():
        parts = [p.strip() for p in re.split(r"[,\n\r]+", root_input) if p.strip()]
        # Keep only IRIs that exist in the parsed graph
        matched = [p for p in parts if p in all_nodes]
        if matched:
            roots = matched
        else:
            st.warning(
                "Keine der eingegebenen Root-IRIs wurden im Graphen gefunden. Zeige stattdessen die ersten Knoten."
            )
            roots = list(all_nodes.keys())[:20]
    else:
        roots = list(all_nodes.keys())[:20]

    initial_nodes: list[dict] = []
    for root in roots[:12]:
        nd = all_nodes[root].copy()
        initial_nodes.append(nd)

    nodes_json = json.dumps(initial_nodes)
    outgoing_json = json.dumps(outgoing_triples)
    node_meta = {k: v.get("meta", {}) for k, v in all_nodes.items()}
    node_meta_json = json.dumps(node_meta)

    if not initial_nodes:
        st.warning(
            "Keine Knoten im Graphen gefunden — überprüfe, ob die TTL korrekt geparst wurde."
        )
        try:
            st.markdown("**TTL (erste 2000 Zeichen)**")
            st.code(ttl_content[:2000])
        except Exception:
            pass

    # Inject JSON safely into HTML template
    html = GRAPH_HTML.replace("{nodes_json}", nodes_json)
    html = html.replace("{outgoing_json}", outgoing_json)
    html = html.replace("{node_meta_json}", node_meta_json)

    # Render
    components.html(html, height=750)
    # Show simple metrics
    st.subheader("Graph summary")
    st.write(
        f"Nodes: {len(all_nodes)} — Roots shown: {len(initial_nodes)} — Outgoing entries: {len(outgoing_triples)}"
    )

st.markdown("---")
st.caption("🐍 RDFLib + Vis.js | `pip install streamlit rdflib` | 🔬 Hierarchical RDF exploration")
