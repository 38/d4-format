#include <stdio.h>
#include <d4.h>
#include <string.h>

typedef struct {
    const char* chrom;
    uint32_t start;
    uint32_t end;
    int* buffer;
} load_request_t;

typedef struct {
    int* buf;
    size_t count;
} task_ctx_t;

void* init(d4_task_part_t* handle, void* extra_data)
{
    load_request_t const* load_req = (load_request_t const*)extra_data;
	task_ctx_t* ret = (task_ctx_t*)malloc(sizeof(task_ctx_t));

    ret->buf = NULL;
    ret->count = 0;

    char buf[20];
	uint32_t l, r;
	d4_task_chrom(handle, buf, sizeof(buf));
	d4_task_range(handle, &l, &r);

    if(strcmp(buf, load_req->chrom) == 0 && l < load_req->end && load_req->start < r)
    {
        ret->buf = load_req->buffer + (l - load_req->start);
        ret->count = (r < load_req->end ) ? r - l : load_req->end - l;
    }
	return ret;
}

int proc(d4_task_part_t* handle, void* task_context, void* extra_data) 
{
	uint32_t l,r;
	d4_task_range(handle, &l, &r);

	task_ctx_t* result = (task_ctx_t*)task_context;

	size_t count;
	for(count = 0; l < r && count < result->count; ) 
	{
		int actual_read = d4_task_read_values(handle, l, result->buf + count, result->count - count);
		l += actual_read;
	}
	return 0;
}

int clean(d4_task_part_result_t* tasks, size_t count, void* extra) 
{
	size_t i;
    for(i = 0; i < count; i++)
        free(tasks[i].task_context);

	return 0;
}

ssize_t parallel_load_chromosome(d4_file_t* fp, char const* chrom, int** data_buf)
{
    d4_file_metadata_t hdr = {};
    d4_file_load_metadata(fp, &hdr);

    int i;
    for(i = 0; i < hdr.chrom_count; i ++) 
    {
        if(strcmp(hdr.chrom_name[i], chrom) == 0)
            break;
    }

    if(i == hdr.chrom_count) return -1;

    size_t chrom_size = hdr.chrom_size[i];

    load_request_t req = {
        .chrom = chrom,
        .start = 0,
        .end = chrom_size,
        .buffer = (int*)calloc(sizeof(int), chrom_size)
    };

    d4_task_desc_t task = {
        .mode = D4_TASK_READ,
        .part_size_limit = 1000000,
        .num_cpus = 8,
        .part_context_create_cb = init,
        .part_process_cb = proc,
        .part_finalize_cb = clean,
        .extra_data = &req
    };

    d4_file_run_task(fp, &task);

    *data_buf = req.buffer;
    return chrom_size;
}



int main(int argc, char** argv) 
{
	if(argc != 3) {
		printf("Usage: %s <input.d4> <chr-name>\n", argv[0]);
		return 1;
	}

	d4_file_t* fp = d4_open(argv[1], "r");

    int* data = NULL;
    ssize_t chrom_size = parallel_load_chromosome(fp, argv[2], &data);
    free(data);
	
    d4_close(fp);
	return 0;
}
