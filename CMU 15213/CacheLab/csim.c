#include "cachelab.h"
#include <getopt.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdio.h>
#include <stddef.h>

typedef struct
{
   int valid_bits;
   unsigned tag;
   int stamp;
} cache_line;




char* filepath=NULL;
int s, E, b, S;
int hit =0, miss=0, eviction=0;
cache_line** cache=NULL;

void init(){
    cache= (cache_line**)malloc(sizeof(cache_line*)*S);
    for(int i=0;i<S;i++){
        *(cache+i)=(cache_line*) malloc(sizeof(cache_line)*E);
    }
    for(int i=0;i<S;i++){
        for(int j=0;j<E;j++){
            // init the valid-bits to zero
            cache[i][j].valid_bits=0; 
            cache[i][j].tag=0xffffffff;
            cache[i][j].stamp=0;
        }
    }
}


void update(unsigned address){
    unsigned s_address =(address>>b) &((0xffffffff)>>(32-s));
    unsigned t_address= address>>(s+b);
    // justify if tag is same, then hit;
    for(int i=0; i<E;i++){
        if((*(cache+s_address)+i)->tag ==t_address){
            cache[s_address][i].stamp=0;
            hit++;
            return;
        }
    }
    // if not hit, if there empty cache_line, then put it here
    for(int i=0;i<E;i++){
        if(cache[s_address][i].valid_bits==0){
            cache[s_address][i].tag=t_address;
            cache[s_address][i].valid_bits=1;
            cache[s_address][i].stamp=0;
            miss++;
            return;
        }
    }

    // If full, then LRU 
    int max_stamp=0;
    int max_i;
    for(int i=0;i<E;i++){
        if(cache[s_address][i].stamp>max_stamp){
            max_stamp=cache[s_address][i].stamp;
            max_i=i;
        }
    }
    eviction++;
    miss++;
    cache[s_address][max_i].tag=t_address;
    cache[s_address][max_i].stamp=0;
}

void time(){
    for(int i=0;i<S;i++){
        for(int j=0;j<E;j++){
            if(cache[i][j].valid_bits==1){
                cache[i][j].stamp++;
            }
        }
    }
}

int main(int argc, char *argv[])
{
    int opt;
    while((opt=getopt(argc, argv, "s:E:b:t:"))!=-1){
        switch (opt){
        case 's':
            s = atoi(optarg);
            break;
        case 'E':
            E = atoi(optarg);
            break;
        case 'b':
            b = atoi(optarg);
            break;
        case 't':
            filepath=optarg;
            break;
        }
    }
    S=1<<s;
    init();
    FILE* file=fopen(filepath,"r");
    if(file==NULL){
        printf("Open file wrong");
        exit(-1);
    }
    char operation;
    unsigned address;
    int size;


    while(fscanf(file," %c %x,%d",&operation,&address,&size)>0){
		switch(operation){
			case 'L':
				update(address);
				break;
			case 'M':
				update(address);
                update(address);
                break;
       
			case 'S':
				update(address);
				break;
		}
		time();
	}

    for(int i=0; i<S;i++){
        free(*(cache+i));
    }

    free(cache);
    fclose(file);
    printSummary(hit, miss, eviction);
    return 0;
}
