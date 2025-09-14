# clusterpy
A simple tcp/tls clustering framework for messaging nodes in pure python

## Synopsis

node1
```./cluster.py -s node1:192.168.0.5:1234 node2:192.168.0.6:5432 node3:192.168.0.7:6657```

node2
```./cluster.py -s node2:192.168.0.6:5432 node1:192.168.0.5:1234 node3:192.168.0.7:6657```

node3
```./cluster.py -s node3:192.168.0.7:6657 node1:192.168.0.5:1234 node3:192.168.0.7:6657```

Here, 3 nodes are put into a cluster. The topology is a mesh, with the -s argument to each
node informing it of which node is "itself", and then which other nodes to trust, and 
which connect ports to use.

## Usage in code

Coming - baby steps
