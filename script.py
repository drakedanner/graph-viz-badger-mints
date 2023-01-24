import os
from shroomdk import ShroomDK
from dotenv import load_dotenv
import networkx as nx
from query import QUERY

# load in SDK API Key
config = load_dotenv()
SHROOMDK_API_KEY = os.environ.get("SHROOMDK_API_KEY")
sdk = ShroomDK(SHROOMDK_API_KEY)

# create an empty NetworkX graph
import networkx as nx 
G = nx.DiGraph()

# get data
sql = QUERY
query_result_set = sdk.query(
    sql,
    timeout_minutes=10,
    ttl_minutes=60
)

# place nodes into directional graph
for record in query_result_set.records:
        G.add_node(record['source']) 
        G.add_node(record['target'])
        if G.has_edge(record['source'], record['target']):
            # edge already exists, increase weight by one
            G[record['source']][record['target']]['weight'] += 1
        else:
            # add new edge with weight 1
            G.add_edge(record['source'], record['target'], weight = 1)

# print values
G_nodes = G.number_of_nodes()
G_edges = G.number_of_edges()
print("Nodes = ", G_nodes, " Edges = ",G_edges)

# convert the NetworkX graph to a gexf file (Graph Exchange XML Format) and store it in our file directory
nx.write_gexf(G, "./data/z-test.gexf")


# Iterate over the results