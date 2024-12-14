from collections import deque, defaultdict

def build_inverse_graph_from_dependencies(dependencies):
    graph = defaultdict(list)
    in_degree = defaultdict(int)
    
    for node, neighbors in dependencies.items():
        for neighbor in neighbors:
            graph[neighbor].append(node)
            in_degree[node] += 1
        if node not in in_degree:
            in_degree[node] = 0
    
    return graph, in_degree

def calculate_groupings_with_topo_sort(dependency_graph):
    graph, in_degree = build_inverse_graph_from_dependencies(dependency_graph)
    
    # Initialize the queue with nodes having zero in-degree
    queue = deque([node for node in in_degree if in_degree[node] == 0])
    result = []

    while queue:
        level = []

        for _ in range(len(queue)):
            node = queue.popleft()
            level.append(node)

            for neighbor in graph[node]:
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)

        result.append(level)

    # Check for cycles in the graph
    if any(in_degree[node] > 0 for node in in_degree):
        raise ValueError("Graph has at least one cycle. Aborting query processing.")

    return result