#include <stdio.h>
#include <d4.h>

int main(int argc, char** argv) 
{
	if(argc != 2) {
		printf("Usage: %s <input.d4>\n", argv[0]);
		return 1;
	}

	d4_file_t* fp = d4_open(argv[1], "r");

	d4_file_metadata_t mt = {};
	d4_file_load_metadata(fp, &mt);

	int i;
	for(i = 0; i < mt.chrom_count; i ++)
		printf("# %s %d\n", mt.chrom_name[i], mt.chrom_size[i]);
	
	d4_file_metadata_clear(&mt);

	for(;;) {

		d4_interval_t data[20000];
		
		char chr[20];
		uint32_t pos;
		
		d4_file_tell(fp, chr, 20, &pos);
		
		ssize_t count = d4_file_read_intervals(fp, data, 20000);

		if(count <= 0) break;

		for(i = 0; i < count; i ++) 
			printf("%s %d %d %d\n", chr, data[i].left, data[i].right, data[i].value);
	}

	d4_close(fp);
	return 0;
}
