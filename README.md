# df-downloader
##Damn Fast Downloader
##A Python-based **parallel-and-distributed** downloader on a Local network. 

##Features
1. - [x] **Peer discovery** on the local network.
    * Currently using broadcast.
    >To-do: **Maintain peer-list** instead of broadcast.
2.  - [x] **Distributing load** among *threads*.
3.  - [] *Network Diagnostics*
4.  - [x] Interactive front-end.
5.  - [x] Fast Peer-discovery
    >To-do: **Distributed Database required to maintain completed downloads**
6.  -[] Temporary Files stored.
    >To-do: Add offline-maintenance and resume support.

##Issues

1. The last few fragments of the file take time to download.
2. HTTP requests to synchronize the final file has network overheads. 
