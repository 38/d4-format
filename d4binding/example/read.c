// This example demostrate how to reading a D4 file

#include <stdio.h>
#include <d4.h>

int main(int argc, char** argv) 
{
	if(argc != 2) {
		printf("Usage: %s <Path/URL>\n", argv[0]);
		return 1;
	}
	
	// Open a D4 file
	d4_file_t* fp = d4_open(argv[1], "r");

	// Read the metadata living in this file. 
	// Please note in order to avoid memory leakage, you must call d4_file_metadata_clear to release
	// all the internal memory allocated to hold the metadata.
	d4_file_metadata_t mt = {};
	d4_file_load_metadata(fp, &mt);

	// Print out information of each chromosome
	int i;
	for(i = 0; i < mt.chrom_count; i ++)
		printf("# %s %d\n", mt.chrom_name[i], mt.chrom_size[i]);

	// Release the memory allocated for metadata
	d4_file_metadata_clear(&mt);

	for(;;) {

		int data[20000];
		char chr[20];

		uint32_t pos;

		// Get the current cursor location
		d4_file_tell(fp, chr, sizeof(chr), &pos);

		// Read the values from the file
		ssize_t count = d4_file_read_values(fp, data, sizeof(data) / sizeof(data[0]));

		// If the count is less than 0, it means we are reaching the end of file
		// To check if there's an error, you can use d4_error_num() to check if there's any error code is set
		if(count <= 0) break;

		// Print out the value one by one
		for(i = 0; i < count; i ++) 
			printf("%s %d %d\n", chr, pos + i, data[i]);
	}

	// Close the D4 file and release all the memory allocated for reading this file
	d4_close(fp);
	return 0;
}
