#include <htslib/bgzf.h>
#include <stdio.h>

int main(int argc, char** argv)
{
	BGZF* fp = bgzf_open(argv[1], "r");
	bgzf_set_cache_size(fp, 102400);
	char buffer[4097] = {};

	int bytes_can_read = 0;
	int bytes_read = 0;
	double sum = 0;
	double size = 0;
	while((bytes_read = bgzf_read(fp, buffer + bytes_can_read, 4096 - bytes_can_read)) > 0) 
	{
		bytes_can_read += bytes_read;

		const char* ptr;
		const char* last_line = buffer;
		int ws_count = 0;
		long long begin, end, value;
		char name[32];
		for(ptr = buffer; ptr < buffer + bytes_can_read; ptr ++) 
		{
			if(*ptr == '\n') 
			{
				sscanf(last_line, "%s%lld%lld%lld", name, &begin, &end, &value);
				sum += value * (end - begin);
				size += end - begin;
				last_line = ptr + 1;
				ws_count = 0;
			}
		}

		bytes_can_read -= last_line - buffer;

		memmove(buffer, last_line, bytes_can_read);
	}

	bgzf_close(fp);

	printf("%lf\n", sum / size);

	return 0;
}
