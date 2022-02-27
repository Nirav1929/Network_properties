import sys
import time
import networkx as nx
from networkx.algorithms.components import number_connected_components
from networkx.classes.function import subgraph
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import functions
from graphframes import *
from copy import deepcopy

sc = SparkContext("local", "degree.py")
sqlContext = SQLContext(sc)


def articulations(g, usegraphframe=False):
    # Get the starting count of connected components
    # YOUR CODE HERE

    count = g.connectedComponents().select('component').distinct().count()

    # Default version sparkifies the connected components process
    # and serializes node iteration.
    if usegraphframe:

        # Get vertex list for serial iteration
        vertices = g.vertices.map(lambda itr: itr.id).collect()
        # For each vertex, generate a new graphframe missing that vertex
        # and calculate connected component count. Then append count to
        # the output
        results = []
        for vertex in vertices:
            sub_edges = g.edges.filter("src!='" + vertex + "'").filter("dst!='" + vertex + "'")
            sub_vertices = g.vertices.filter("id!='" + vertex + "'")
            sub_gf = GraphFrame(sub_vertices, sub_edges)
            curr_count = new_gf.connectedComponents().select('component').distinct().count()
            if curr_count>count:
                results.append((vertex,1))
            else:
                results.append((vertex,0))
        return sqlContext.createDataFrame(sc.parallelize(results),['id', 'articulation'])

    # Non-default version sparkifies node iteration and uses networkx
    # for connected components count.
    else:
	results = []
	graph = nx.Graph()
	vertices = g.vertices.map(lambda vertex: vertex.id).collect()
	edges = g.edges.map(lambda edge: (edge.src, edge.dst)).collect()
	graph.add_edges_from(edges)
  	graph.add_nodes_from(vertices)
        for vertex in vertices:
            curr_graph = graph.copy()
            curr_graph.remove_node(vertex)
            curr_count = nx.number_connected_components(curr_graph)
            if curr_count>count:
                results.append((vertex,1))
            else:
                results.append((vertex,0))
        return sqlContext.createDataFrame(sc.parallelize(results),['id','articulation'])

filename = sys.argv[1]
lines = sc.textFile(filename)

pairs = lines.map(lambda s: s.split(","))
e = sqlContext.createDataFrame(pairs, ['src', 'dst'])
e = e.unionAll(e.selectExpr('src as dst', 'dst as src')).distinct()  # Ensure undirectedness

# Extract all endpoints from input file and make a single column frame.
v = e.selectExpr('src as id').unionAll(e.selectExpr('dst as id')).distinct()

# Create graphframe from the vertices and edges.
g = GraphFrame(v, e)

# Runtime approximately 5 minutes
print("---------------------------")
print("Processing graph using Spark iteration over nodes and serial (networkx) connectedness calculations")
init = time.time()
df = articulations(g, False)
print("Execution time: %s seconds" % (time.time() - init))
print("Articulation points:")
df.filter('articulation = 1').show(truncate=False)
print("---------------------------")

# write to csv file
df.filter('articulation = 1').toPandas().to_csv('articulation_out.csv')

# Runtime for below is more than 2 hours
print("Processing graph using serial iteration over nodes and GraphFrame connectedness calculations")
init = time.time()
df = articulations(g, True)
print("Execution time: %s seconds" % (time.time() - init))
print("Articulation points:")
df.filter('articulation = 1').show(truncate=False)
