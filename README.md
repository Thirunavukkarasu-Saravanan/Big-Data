# Big-Data
Set of projects implemeting map-reduction and Graph processing using different platforms such as Hadoop, Spark, Hive, Pig and GraphX
Concentration on two problems to solve using map reduce with different technologies in Big data.

HISTOGRAM PIXEL MAP REDUCE

A pixel in an image can be represented using 3 colors: red, green, and blue, where each color intensity is an integer between 0 and 255. 
In this project, a Map-Reduce program that derives a histogram for each color. 
For red, for example, the histogram will indicate how many pixels in the dataset have a green value equal to 0, equal to 1, etc (256 values). 
The pixel file is a text file that has one text line for each pixel.
For example, the line

23,140,45
represents a pixel with red=23, green=140, and blue=45.

GRAPH PROCESSING OF CONNECTED COMPONENTS

An undirected graph is represented in the input text file using one line per graph vertex. For example, the line

1,2,3,4,5,6,7

represents the vertex with ID 1, which is connected to the vertices with IDs 2, 3, 4, 5, 6, and 7. For example, the following graph:
is represented in the input file as follows:
3,2,1
2,4,3
1,3,4,6
5,6
6,5,7,1
0,8,9
4,2,1
8,0
9,0
7,6

 A Map-Reduce program that finds the connected components of any undirected graph and prints the size of these connected components is written
 A connected component of a graph is a subgraph of the graph in which there is a path from any two vertices in the subgraph. 
 For the above graph, there are two connected components: one 0,8,9 and another 1,2,3,4,5,6,7. 
 The program prints the sizes of these connected components: 3 and 7.
 
 GENERAL ASPECTS
 
 Results - The output contains results (part-0000*) and log files (local.out, distr.out) which are config files for the same program made to run in both local and distributed environments.
 Log Files - Some log files contains results written to them directly.
 Output files - The reduced output from both local and distributed mode can be found here 

