/*
*
* ECE 6130 Big Data and Cloud Computing
* Spring 2019
* Project code: Highly Optimized Parallel and Distributed Breadth First Search on Graphic Processing Units
* Name: Tianyu Yang
* GW ID:G38878678
* Referenced from https://siddharths2710.wordpress.com/2017/05/16/implementing-breadth-first-search-in-cuda/
*
*/

#include "cuda_runtime.h"
#include "device_launch_parameters.h"
#include <cuda.h>
#include <device_functions.h>
#include <cuda_runtime_api.h>

#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <conio.h>
#include <iostream>
#include <ctime>
#include <ratio>
#include <chrono>

#define NUM_NODES 99999999//1023
#define parameter 511//511
#define time 1

using namespace std;
int n, r;
double d;
FILE *f;

typedef struct
{
	int start;     // Index of first adjacent node in Ea	
	int length;    // Number of adjacent nodes 
} Node;

// Define the structure of node

__global__ void CUDA_BFS_KERNEL(Node *Va, int *Ea, bool *Fa, bool *Xa, int *Ca, bool *done)
{

	int id = threadIdx.x + blockIdx.x * blockDim.x;
	if (id > NUM_NODES)
		*done = false;


	if (Fa[id] == true && Xa[id] == false)
	{
		printf("%d ", id); //This printf gives the order of vertices in BFS	
		Fa[id] = false;
		Xa[id] = true;
		__syncthreads();
		int k = 0;
		int i;
		int start = Va[id].start;
		int end = start + Va[id].length;
		for (int i = start; i < end; i++)
		{
			int nid = Ea[i];

			if (Xa[nid] == false)
			{
				Ca[nid] = Ca[id] + 1;
				Fa[nid] = true;
				*done = false;
			}

		}

	}

}

// The BFS frontier corresponds to all the nodes being processed at the current level.

int main()
{
	Node node[NUM_NODES];
	//int edgesSize = 2 * NUM_NODES;
	int edges[NUM_NODES];

	int a[NUM_NODES];
	int tmp[NUM_NODES];
	char fileName[] = "web-Google.txt";
	f = fopen(fileName, "r");
	n = 0;
	while (1) {
		r = fscanf(f, "%lf", &d);
		if (1 == r) {
			n++;
			//printf("[%d]==%lg\n", n-1, d);
			a[n - 1] = (int)d;
		}
		else if (0 == r) {
			fscanf(f, "%*c");
		}
		else break;
	}

	//number of nodes and edges
	int n = a[1];
	int e = a[2];
	Node node[NUM_NODES];
	//int edgesSize = 2 * NUM_NODES;
	int edges[NUM_NODES];
	cout << "No. of nodes = " << n << endl;
	cout << "No. of edges = " <<e << endl;
	for (int i = 3; i < 2 * e + 3; i++) {
		if (i % 2 == 0) {
			edges[i / 2 - 2] = a[i];
		}
		if (i % 2 == 1) {
			tmp[(i - 1) / 2 - 1] = a[i];
		}
	}
	for (int i = 0; i < n; i++) {
		node[i].length = 0;
	}
	for (int i = 0; i < e; i++) {
		//cout << edges[i] << endl;
		//cout <<tmp[i] << endl;
		if (node[tmp[i]].start != 0) {
			node[tmp[i]].start = i;
		}
		node[tmp[i]].length++;
	}
	for (int i = 0; i < n; i++) {
		//cout << node[i].start << endl;
		//cout << node[i].length << endl;
	}
	fclose(f);

	// Special graph nodes
	/*for (int i = 0; i < parameter; i++) {
		node[i].start = 2*i;
		node[i].length = 2;
	}
	for (int i = parameter; i < NUM_NODES; i++) {
		node[i].start = i+1;
		node[i].length = 0;
	}
	for (int i = 0; i < NUM_NODES; i++) {
		edges[i] = i+1;
	}*/

	 //Eg. 1
	/*node[0].start = 0;
	node[0].length = 2;

	node[1].start = 2;
	node[1].length = 1;

	node[2].start = 3;
	node[2].length = 1;

	node[3].start = 4;
	node[3].length = 1;

	node[4].start = 5;
	node[4].length = 0;

	edges[0] = 1;
	edges[1] = 2;
	edges[2] = 4;
	edges[3] = 3;
	edges[4] = 4;*/

	// Eg. 2
	 /*node[0].start = 0;
	 node[0].length = 2;

	 node[1].start = 2;
	 node[1].length = 2;

	 node[2].start = 4;
	 node[2].length = 2;

	 node[3].start = 6;
	 node[3].length = 2;

	 node[4].start = 5;
	 node[4].length = 0;

	 edges[0] = 1;
	 edges[1] = 2;	
	 edges[2] = 0;
	 edges[3] = 3;
	 edges[4] = 0;
	 edges[5] = 3;
	 edges[6] = 1;
	 edges[7] = 2;*/

	bool frontier[NUM_NODES] = { false };
	bool visited[NUM_NODES] = { false };
	int cost[NUM_NODES] = { 0 };

	int source = 0;
	frontier[source] = true;

	Node* Va;
	cudaMalloc((void**)&Va, sizeof(Node)*NUM_NODES);
	cudaMemcpy(Va, node, sizeof(Node)*NUM_NODES, cudaMemcpyHostToDevice);

	int* Ea;
	cudaMalloc((void**)&Ea, sizeof(Node)*NUM_NODES);
	cudaMemcpy(Ea, edges, sizeof(Node)*NUM_NODES, cudaMemcpyHostToDevice);

	bool* Fa;
	cudaMalloc((void**)&Fa, sizeof(bool)*NUM_NODES);
	cudaMemcpy(Fa, frontier, sizeof(bool)*NUM_NODES, cudaMemcpyHostToDevice);

	bool* Xa;
	cudaMalloc((void**)&Xa, sizeof(bool)*NUM_NODES);
	cudaMemcpy(Xa, visited, sizeof(bool)*NUM_NODES, cudaMemcpyHostToDevice);

	int* Ca;
	cudaMalloc((void**)&Ca, sizeof(int)*NUM_NODES);
	cudaMemcpy(Ca, cost, sizeof(int)*NUM_NODES, cudaMemcpyHostToDevice);

	int num_blks = 1;
	int threads = 5;

	bool done;
	bool* d_done;
	cudaMalloc((void**)&d_done, sizeof(bool));
	printf("\n\n");
	int count = 0;

	printf("Threads Order: \n\n");

	using namespace std::chrono;
	auto start = high_resolution_clock::now();

	// Run n times for Bfs program
	for (int i = 0; i < time; i++) {
		do {
			count++;
			done = true;
			cudaMemcpy(d_done, &done, sizeof(bool), cudaMemcpyHostToDevice);
			CUDA_BFS_KERNEL << <num_blks, threads >> > (Va, Ea, Fa, Xa, Ca, d_done);
			cudaMemcpy(&done, d_done, sizeof(bool), cudaMemcpyDeviceToHost);

		} while (!done);
	}

	auto stop = high_resolution_clock::now();
	auto duration = duration_cast<microseconds>(stop - start);
	std::cout << "\nTime taken: " << duration.count() << " us" << std::endl;

	cudaMemcpy(cost, Ca, sizeof(int)*NUM_NODES, cudaMemcpyDeviceToHost);

	printf("\nNumber of threads used : %d \n", count);


	printf("\nThreads for each node: ");
	for (int i = 0; i < NUM_NODES; i++)
		printf("%d    ", cost[i]);
	printf("\n");
	_getch();
	system("pause");

}