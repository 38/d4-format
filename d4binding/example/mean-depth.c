#include <stdio.h>
#include <d4.h>
#include <string.h>

typedef struct {
	char name[20];
	uint32_t count;
	double sum;
} task_ctx_t;
void* init(d4_task_part_t* handle, void* extra_data) 
{
	task_ctx_t* ret = (task_ctx_t*)malloc(sizeof(task_ctx_t));
	d4_task_chrom(handle, ret->name, 20);
	uint32_t l, r;
	d4_task_range(handle, &l, &r);
	ret->count = r - l;
	ret->sum = 0;
	return ret;
}

int proc(d4_task_part_t* handle, void* task_context, void* extra_data) 
{
	uint32_t l,r;
	d4_task_range(handle, &l, &r);

	task_ctx_t* result = (task_ctx_t*)task_context;

	uint32_t pos;
	for(pos = l; pos < r; ) 
	{
		int32_t buffer[10000];
		int count = d4_task_read_values(handle, pos, buffer, sizeof(buffer) / sizeof(*buffer));
		int i;
		for(i = 0; i < count ; i ++)
			result->sum += buffer[i];
		pos += count;
	}
	return 0;
}

int task_cmp(const void* a, const void* b) 
{
	task_ctx_t* ctx_a = (task_ctx_t*)((d4_task_part_result_t*)a)->task_context;
	task_ctx_t* ctx_b = (task_ctx_t*)((d4_task_part_result_t*)b)->task_context;
	return strcmp(ctx_a->name, ctx_b->name);
}
int clean(d4_task_part_result_t* tasks, size_t count, void* extra) 
{
	size_t i;
	qsort(tasks, count, sizeof(d4_task_part_result_t), task_cmp);
	char current_chrom[20] = {};
	double sum = 0;
	double base_count = 0;
	for(i = 0; i <= count; i ++) 
	{
		task_ctx_t* ctx = i == count ? NULL : (task_ctx_t*)tasks[i].task_context;
		if(ctx == NULL || strcmp(current_chrom, ctx->name) != 0) 
		{
			if(current_chrom[0])
				printf("%s %lf\n", current_chrom, sum / base_count);
			if(ctx == NULL)
				break;
			sum = 0;
			base_count = 0;
			memcpy(current_chrom, ctx->name, 20);
		}
		sum += ctx->sum;
		base_count += ctx->count;
		free(ctx);
	}

	return 0;
}



int main(int argc, char** argv) 
{
	if(argc != 2) {
		printf("Usage: %s <input.d4>\n", argv[0]);
		return 1;
	}

	d4_file_t* fp = d4_open(argv[1], "r");

	d4_task_desc_t task = {
		.mode = D4_TASK_READ,
		.part_size_limit = 10000000,
		.num_cpus = 8,
		.part_context_create_cb = init,
		.part_process_cb = proc,
		.part_finalize_cb = clean,
		.extra_data = NULL
	};

	d4_file_run_task(fp, &task);

	d4_close(fp);
	return 0;
}
