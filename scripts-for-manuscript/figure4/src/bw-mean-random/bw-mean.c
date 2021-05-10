#include <bigWig.h>
#include <stdio.h>
void print_item(bigWigFile_t* fp, const char* name, int left, int right) 
{
	double sum = 0;
	double count = right - left;
	for(;left < right;) {
		int new_left = left + 1024;
		if(new_left > right) new_left = right;
		bwOverlapIterator_t *iter = bwOverlappingIntervalsIterator(fp, name, left, new_left, new_left - left);
		if(NULL == iter) break;
		while(iter->data) {
			int i;
			for(i=0; i<iter->intervals->l; i++) {
				sum += (iter->intervals->end[i] - iter->intervals->start[i]) * iter->intervals->value[i];
			}
			iter = bwIteratorNext(iter);
		}
		bwIteratorDestroy(iter);
		left = new_left;
	}
	printf("%s %d %d %lf\n", name, left, right, sum / count);
}
int main(int argc, char** argv) 
{
	bwInit(1<<17);
	bigWigFile_t *fp = bwOpen(argv[1], NULL, "r");

	FILE* fp_bed = fopen(argv[2], "r");
	char name[30];
	int left, right;
	while(EOF != fscanf(fp_bed, "%s%d%d", name, &left, &right))
		print_item(fp, name, left, right);

	return 0;
}

