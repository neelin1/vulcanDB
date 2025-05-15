from collections import deque
from typing import List, Dict, Set, Deque

from vulcan.utils.llm_helpers import TableTraitsWithName, DependencyDetail


def determine_table_creation_order(
    table_traits_list: List[TableTraitsWithName], initial_table_list: List[str]
) -> List[str]:
    """
    Determines the correct order for table creation based on dependencies
    defined in table_traits.

    Args:
        table_traits_list: A list of TableTraitsWithName objects, each describing
                           a table and its dependencies.
        initial_table_list: The initial list of table names generated earlier
                            in the process, used for verification.

    Returns:
        A list of table names in the correct creation order.

    Raises:
        ValueError: If a cyclic dependency is detected, or if the resolved
                    tables do not match the initial_table_list, or if
                    input lists are inconsistent.
    """
    if not table_traits_list:
        if not initial_table_list:
            return []  # No tables, no traits, no order.
        else:
            # Traits are needed for dependencies, cannot proceed.
            raise ValueError(
                "Table traits list is empty, but initial table list is not. "
                "Cannot determine creation order without dependency information."
            )

    adj: Dict[str, Set[str]] = {}  # Graph: Parent -> Set of children (dependents)
    in_degree: Dict[str, int] = {}  # In-degree for each table (child)

    # Accumulate all unique table names from traits (both table.name and dependencies.parent_table_name)
    # These are all the nodes that will be part of our dependency graph.
    all_potential_nodes: Set[str] = set()
    for traits in table_traits_list:
        all_potential_nodes.add(traits.name)
        adj.setdefault(traits.name, set())
        in_degree.setdefault(traits.name, 0)
        for dep in traits.dependencies:
            all_potential_nodes.add(dep.parent_table_name)
            adj.setdefault(dep.parent_table_name, set())
            in_degree.setdefault(dep.parent_table_name, 0)

    # Verify that the universe of tables from traits matches initial_table_list
    expected_tables_set = set(initial_table_list)

    if all_potential_nodes != expected_tables_set:
        missing_in_traits = expected_tables_set - all_potential_nodes
        extra_in_traits = all_potential_nodes - expected_tables_set
        error_messages = []
        if missing_in_traits:
            error_messages.append(
                f"Tables {missing_in_traits} are in initial_table_list but not found "
                f"as a table name or dependency parent in table_traits."
            )
        if extra_in_traits:
            error_messages.append(
                f"Tables {extra_in_traits} are mentioned in table_traits (as table name or dependency parent) "
                f"but are not in initial_table_list."
            )
        raise ValueError(
            "Mismatch between tables in traits and initial_table_list: "
            + " ".join(error_messages)
        )

    # Now, all_potential_nodes is the definitive set of tables to order,
    # and it's identical to expected_tables_set.

    # Build the graph (adjacency list: parent -> children) and calculate in-degrees
    for traits in table_traits_list:
        # traits.name is the child table in the context of its dependencies
        child_table = traits.name
        for dep in traits.dependencies:
            parent_table = dep.parent_table_name

            # Add edge from parent to child if not already present
            if child_table not in adj[parent_table]:
                adj[parent_table].add(child_table)
                in_degree[child_table] += 1

    # Topological sort (Kahn's algorithm)
    # Initialize queue with all tables that have an in-degree of 0 (no prerequisites)
    queue: Deque[str] = deque()
    for table_name in all_potential_nodes:  # Iterate over the consistent set of tables
        if in_degree[table_name] == 0:
            queue.append(table_name)

    creation_order: List[str] = []
    while queue:
        current_table = queue.popleft()
        creation_order.append(current_table)

        # For each child (dependent_table) of the current_table
        # Sort children for deterministic output if multiple orders are possible
        for dependent_table in sorted(list(adj.get(current_table, set()))):
            in_degree[dependent_table] -= 1
            if in_degree[dependent_table] == 0:
                queue.append(dependent_table)

    # Verification: Check for cycles
    # If a cycle exists, not all tables will be in creation_order.
    if len(creation_order) != len(all_potential_nodes):
        processed_tables = set(creation_order)
        missing_or_cycled = all_potential_nodes - processed_tables
        raise ValueError(
            f"Cyclic dependency detected or some tables could not be ordered. "
            f"Problem tables: {missing_or_cycled}. "
            f"Processed count: {len(creation_order)}, Expected count: {len(all_potential_nodes)}"
        )

    # The final creation_order list contains all tables from initial_table_list,
    # sorted according to their dependencies.
    return creation_order
