#include <d4.h>
int main() 
{
	d4_file_t* fp = d4_open("/tmp/test.d4", "w");
	static char* chrom_list[] = {"chr1", "chr2"};
	static uint32_t size_list[] = {10000, 20000};
	d4_file_metadata_t hdr = {
		.chrom_count = 2,
		.chrom_name = chrom_list,
		.chrom_size = size_list,
		.dict_type = D4_DICT_SIMPLE_RANGE,
		.dict_data = {
			.simple_range = {
				.low= 0,
				.high= 4
			}
		}
	};

	d4_file_update_metadata(fp, &hdr);

	int vals[] = {0,1,2,3,4,5,6,7,8,9,10};
	d4_file_write_values(fp, vals, 11);

	d4_file_seek(fp, "chr2", 5000);
	d4_file_write_values(fp, vals, 11);

	// You can't go back
	printf("%d\n", d4_file_seek(fp, "chr1", 0));

	// But you can go to some place after last pos
	printf("%d\n", d4_file_seek(fp, "chr1", 1000));
	d4_file_write_values(fp, vals, 11);

	d4_close(fp);
	return 0;
}
