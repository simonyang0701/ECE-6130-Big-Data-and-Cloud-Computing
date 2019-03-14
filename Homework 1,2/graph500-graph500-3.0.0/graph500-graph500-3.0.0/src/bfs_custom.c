//Stub for custom BFS implementations

#include "common.h"
#include "aml.h"
#include "csr_reference.h"
#include "bitmap_reference.h"
#include <stdint.h>
#include <inttypes.h>
#include <stdlib.h>
#include <stddef.h>
#include <string.h>
#include <limits.h>
#include <assert.h>
#include <stdint.h>

//VISITED bitmap parameters
unsigned long *visited;
int64_t visited_size;

int64_t *pred_glob,*column;
int *rowstarts;
oned_csr_graph g;

unsigned int *q1,*q2;
unsigned int qc,q2c; //pointer to first free element
void make_graph_data_structure(const tuple_graph* const tg) {
	unsigned int i,j,k;
	convert_graph_to_oned_csr(tg, &g);
	column=g.column;
	rowstarts=g.rowstarts;

	visited_size = (g.nlocalverts + ulong_bits - 1) / ulong_bits;
	aml_register_handler(visithndl,1);
	q1 = xmalloc(g.nlocalverts*sizeof(int)); //100% of vertexes
	q2 = xmalloc(g.nlocalverts*sizeof(int));
	for(i=0;i<g.nlocalverts;++i) q1[i]=0,q2[i]=0; //touch memory
	visited = xmalloc(visited_size*sizeof(unsigned long));
}

void run_bfs(int64_t root, int64_t* pred) {
	int64_t nvisited;
	long sum;
	unsigned int i,j,k,lvl=1;
	pred_glob=pred;
	aml_register_handler(visithndl,1);

	CLEAN_VISITED();

	qc=0; sum=1; q2c=0;

	nvisited=1;
	if(VERTEX_OWNER(root) == rank) {
		pred[VERTEX_LOCAL(root)]=root;
		SET_VISITED(root);
		q1[0]=VERTEX_LOCAL(root);
		qc=1;
	} 

	// While there are vertices in current level
	while(sum) {
#ifdef DEBUGSTATS
		double t0=aml_time();
		nbytes_sent=0; nbytes_rcvd=0;
#endif
		//for all vertices in current level send visit AMs to all neighbours
#pragma omp parallel for{
		for(i=0;i<qc;++i)
			for(j=rowstarts[q1[i]];j<rowstarts[q1[i]+1];++j)
				send_visit(COLUMN(j),q1[i]);
}
		aml_barrier();

		qc=q2c;int *tmp=q1;q1=q2;q2=tmp;
		sum=qc;
		aml_long_allsum(&sum);

		nvisited+=sum;

		q2c=0;
#ifdef DEBUGSTATS
		aml_long_allsum(&nbytes_sent);
		t0-=aml_time();
		if(!my_pe()) printf (" --lvl%d : %lld(%lld,%3.2f) visited in %5.2fs, network aggr %5.2fGb/s\n",lvl++,sum,nvisited,((double)nvisited/(double)g.notisolated)*100.0,-t0,-(double)nbytes_sent*8.0/(1.e9*t0));
#endif
	}
	aml_barrier();

}

void get_edge_count_for_teps(int64_t* edge_visit_count) {
	long i,j;
	long edge_count=0;
	for(i=0;i<g.nlocalverts;i++)
		if(pred_glob[i]!=-1) {
			for(j=g.rowstarts[i];j<g.rowstarts[i+1];j++)
				if(COLUMN(j)<=VERTEX_TO_GLOBAL(my_pe(),i))
					edge_count++;
		}
	aml_long_allsum(&edge_count);
	*edge_visit_count=edge_count;
}

//user provided function to initialize predecessor array to whatevere value user needs
void clean_pred(int64_t* pred) {
	int i;
	for(i=0;i<g.nlocalverts;i++) pred[i]=-1;
}

//user provided function to be called once graph is no longer needed
void free_graph_data_structure(void) {
	free_oned_csr_graph(&g);
	free(visited);
}

//user should change is function if distribution(and counts) of vertices is changed
size_t get_nlocalverts_for_pred(void) {
	return g.nlocalverts;
}
