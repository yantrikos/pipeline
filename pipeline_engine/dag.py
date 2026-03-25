"""DAG resolution — topological sort, ready-node detection, cycle detection."""

import logging
from typing import Optional

from pipeline_engine.errors import CycleDetectedError, ValidationError

logger = logging.getLogger("pipeline.dag")


def topological_sort(nodes: dict, edges: list) -> list[str]:
    """
    Kahn's algorithm for topological sort.
    Raises CycleDetectedError if cycle exists.
    Returns ordered list of node IDs.
    """
    in_degree = {nid: 0 for nid in nodes}
    adjacency = {nid: [] for nid in nodes}

    for edge in edges:
        src = edge.get("from", edge.get("source"))
        dst = edge.get("to", edge.get("target"))
        if src in adjacency and dst in in_degree:
            adjacency[src].append(dst)
            in_degree[dst] += 1

    queue = [nid for nid, deg in in_degree.items() if deg == 0]
    result = []

    while queue:
        node = queue.pop(0)
        result.append(node)
        for neighbor in adjacency.get(node, []):
            in_degree[neighbor] -= 1
            if in_degree[neighbor] == 0:
                queue.append(neighbor)

    if len(result) != len(nodes):
        remaining = set(nodes.keys()) - set(result)
        raise CycleDetectedError(
            f"Cycle detected in workflow DAG involving: {remaining}",
            {"cycle_nodes": list(remaining)},
        )

    return result


def get_ready_nodes(
    nodes: dict,
    edges: list,
    node_statuses: dict[str, str],
) -> list[str]:
    """
    Get nodes whose predecessors are all completed and which are still pending.
    """
    # Build predecessor map
    predecessors: dict[str, list[str]] = {nid: [] for nid in nodes}
    for edge in edges:
        src = edge.get("from", edge.get("source"))
        dst = edge.get("to", edge.get("target"))
        if dst in predecessors:
            predecessors[dst].append(src)

    ready = []
    for nid in nodes:
        status = node_statuses.get(nid, "pending")
        if status != "pending":
            continue
        # Check all predecessors completed (or skipped)
        preds = predecessors.get(nid, [])
        if all(node_statuses.get(p, "pending") in ("completed", "skipped", "healed") for p in preds):
            # Check edge conditions
            edge_ok = True
            for edge in edges:
                if edge.get("to", edge.get("target")) == nid:
                    condition = edge.get("condition")
                    if condition:
                        from pipeline_engine.expressions import evaluate_condition
                        src = edge.get("from", edge.get("source"))
                        src_output = node_statuses.get(f"_output_{src}", {})
                        if not evaluate_condition(condition, {"output": src_output}):
                            edge_ok = False
                            break
            if edge_ok:
                ready.append(nid)

    return ready


def validate_workflow(definition: dict) -> list[str]:
    """Validate a workflow definition. Returns list of errors (empty = valid)."""
    errors = []

    nodes = definition.get("nodes", {})
    edges = definition.get("edges", [])

    if not nodes:
        errors.append("Workflow must have at least one node")
        return errors

    # Check all edge references exist
    node_ids = set(nodes.keys())
    for i, edge in enumerate(edges):
        src = edge.get("from", edge.get("source"))
        dst = edge.get("to", edge.get("target"))
        if src not in node_ids:
            errors.append(f"Edge {i}: source '{src}' not found in nodes")
        if dst not in node_ids:
            errors.append(f"Edge {i}: target '{dst}' not found in nodes")

    # Check for cycles
    try:
        topological_sort(nodes, edges)
    except CycleDetectedError as e:
        errors.append(str(e))

    # Check node types
    for nid, node in nodes.items():
        if "type" not in node:
            errors.append(f"Node '{nid}' missing 'type' field")

    return errors
