#include <bigWig.h>
#include <stdio.h>

int main(int argc, char** argv) 
{
	bwInit(1<<17);
	bigWigFile_t *fp = bwOpen(argv[1], NULL, "r");

	int tid, i;
	double sum = 0;
	long long size = 0;
	for(tid = 0; tid < fp->cl->nKeys; tid ++) 
	{
		int len = fp->cl->len[tid];
		const char* name = fp->cl->chrom[tid];
		bwOverlapIterator_t *iter = bwOverlappingIntervalsIterator(fp, name, 0, len, 1024);
		size += len;
		while(iter->data) {
            for(i=0; i<iter->intervals->l; i++) {
				sum += (iter->intervals->end[i] - iter->intervals->start[i]) * iter->intervals->value[i];
            }
			iter = bwIteratorNext(iter);
		}
		bwIteratorDestroy(iter);
		printf("%s %lf\n", name, sum / size);
	}
	return 0;
}

