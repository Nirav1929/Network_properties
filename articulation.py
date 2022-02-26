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
    # og_components = g.connectedComponents().select('component').distinct().count()
    edges = g.edges.map(lambda k: (k.src, k.dst)).collect()
    vertices = g.vertices.map(lambda k: k.id).collect()
    nx_graph = nx.Graph()
    nx_graph.add_edges_from(edges)
    og_components = number_connected_components(nx_graph)
    # Default version sparkifies the connected components process
    # and serializes node iteration.
    if usegraphframe:
        # Get vertex list for serial iteration
        # For each vertex, generate a new graphframe missing that vertex
        # and calculate connected component count. Then append count to
        # the output
        results = []
        for i, v in enumerate(vertices):
            sub_v = g.vertices.filter("id != '{}'".format(v))
            sub_e = g.edges.filter("src != '{}'".format(v)).filter("dst != '{}'".format(v))
            sub_g = GraphFrame(sub_v, sub_e)
            sub_components = sub_g.connectedComponents().select('component').distinct().count()
            results.append((v, 1) if sub_components > og_components else (v, 0))
        return sqlContext.createDataFrame(sc.parallelize(results), ['id', 'articulation'])

    # Non-default version sparkifies node iteration and uses networkx
    # for connected components count.
    else:
        return sqlContext.createDataFrame(g.vertices.map(lambda k: k.id)
                                                    .map(lambda k: (k, number_connected_components(subgraph(nx_graph, [x for x in vertices if x != k]))))
                                                    .map(lambda k: (k[0], 1) if k[1] > og_components else (k[0], 0)),
                                          ['id', 'articulation'])


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
