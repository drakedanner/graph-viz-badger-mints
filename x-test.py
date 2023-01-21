import csv
from tqdm import tqdm
import networkx as nx


# create an empty NetworkX graph
import networkx as nx 
G = nx.DiGraph()


# add nodes and edges based on our CSV file
with open('x-test.csv', 'r') as f:
    data = csv.reader(f)
    headers = next(data)
# iterate over each row adding nodes for operator (first column) and target (second column)
    for row in tqdm(data):
        G.add_node(row[0]) #address in first column
        G.add_node(row[1]) #address in second column
        if G.has_edge(row[0], row[1]):
            # edge already exists, increase weight by one
            G[row[0]][row[1]]['weight'] += 1
        else:
            # add new edge with weight 1
            G.add_edge(row[0], row[1], weight = 1)

# print values
G_nodes = G.number_of_nodes()
G_edges = G.number_of_edges()
print("Nodes = ", G_nodes, " Edges = ",G_edges)

# convert the NetworkX graph to a gexf file (Graph Exchange XML Format) and store it in our file directory
nx.write_gexf(G, "./data/x-test.gexf")
