import sys
sys.path.append('../snappy/')
import snap
import os



def initialize():
	#graph = snap.LoadEdgeListStr(snap.PNGraph, "edges.txt", 0, 1)
	#print graph.GetEdges()
	#print graph.GetNodes()

	G = snap.TNGraph.New()

	### setup the user map ###
	userMap = {}
	with open("users.txt") as f:
   		users = f.read().splitlines()

   	length_users = len(users)
   	for i in xrange(length_users):
   		key = users[i].strip()
   		userMap[key] = i
   		G.AddNode(i)

   	### set up the url map ###
   	sitesMap = {}
	with open("sites.txt") as f2:
   		sites = f2.read().splitlines()

   	length_sites = len(sites)
   	for i in xrange(length_sites):
   		key = sites[i].strip()
   		sitesMap[key] = i+length_users
   		G.AddNode(i+length_users)

   	## Add the edges ###
	edgeA = []
	edgeB = []
	with open("edges.txt") as f3:
		for line in f3.read().splitlines():
			(site, user) = line.split()
			edgeA.append(site)
			edgeB.append(user)

   	for i in xrange(len(edgeA)):
   		siteID = sitesMap[edgeA[i]]
   		userID = userMap[edgeB[i]]
   		G.AddEdge(siteID, userID)

 	### Create the topics map ###
 	sites = []
 	topics = []
 	groundTruth = {}
 	with open("topics.txt") as f4:
 		for line in f4.read().splitlines():
 			(site, topic) = line.split()
 			sites.append(site)
 			topics.append(topic)

 	for i in xrange(len(sites)):
 		groundTruth[sitesMap[sites[i]]] = topics[i]

  	return G, groundTruth


if __name__ == '__main__':
   initialize()