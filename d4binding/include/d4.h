#ifndef __D4_H__
#define __D4_H__
#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>

#ifdef __cplusplus
extern "C" {
#endif

/*!< The handle for a D4 file */
typedef struct d4_file_t d4_file_t;

/*!< Describes what kind of dictionary this d4 file holds */
typedef enum {
	/*!< The dictionary that is defined by a range of values */
    D4_DICT_SIMPLE_RANGE = 0,
	/*!< The dictionary describes by a value map */
    D4_DICT_VALUE_MAP = 1,
} d4_dict_type_t;

/*!< The dictionary data for simple ranage dictionary */
typedef struct {
    int32_t low;
    int32_t high;
 } d4_simple_range_dict_t;

/*!< The dictionary data or value map dictionary */
 typedef struct {
     size_t size;
     int32_t* values;
 } d4_value_map_dict_t;

/*!< The metadata of a D4 file */
typedef struct {
	/*!< Number of chromosomes defined in the file */
	size_t chrom_count;
	/*!< List of chromosome names */
    char** chrom_name;
	/*!< List o fchromosome sizes */
    uint32_t* chrom_size;
	/*!< Dictionary type */
    d4_dict_type_t dict_type;
	/*!< Dictionary data */
    union {
        d4_simple_range_dict_t simple_range;
        d4_value_map_dict_t value_map;
    } dict_data;
} d4_file_metadata_t;

/*!< A value interval */
typedef struct {
    uint32_t left;
    uint32_t right;
    int32_t value;
} d4_interval_t;

/*!< Open a D4 file, mode can be either "r" or "w" */
d4_file_t* d4_open(const char* path, const char* mode);

/*!< Close a opened D4 file */
int d4_close(d4_file_t* handle);

int d4_file_load_metadata(const d4_file_t* handle, d4_file_metadata_t* buf);
int d4_file_update_metadata(d4_file_t* handle, const d4_file_metadata_t* metadata);

static inline void d4_file_metadata_clear(d4_file_metadata_t* meta) 
{
	if(NULL == meta) return;
	int i;
	for(i = 0; i < meta->chrom_count; i ++)
		free(meta->chrom_name[i]);
	free(meta->chrom_name);
	free(meta->chrom_size);
	meta->chrom_name = NULL;
	meta->chrom_size = NULL;
	meta->chrom_count = 0;

	if(meta->dict_type == D4_DICT_VALUE_MAP) {
		meta->dict_data.value_map.size = 0;
		free(meta->dict_data.value_map.values);
		meta->dict_type = D4_DICT_SIMPLE_RANGE;
		meta->dict_data.simple_range.low = 0;
		meta->dict_data.simple_range.high = 1;
	}
}

// The streaming API

ssize_t d4_file_read_values(d4_file_t* handle, int32_t* buf, size_t count);
ssize_t d4_file_read_intervals(d4_file_t* handle, d4_interval_t* buf, size_t count);
ssize_t d4_file_write_values(d4_file_t* handle, const int32_t* buf, size_t count);
ssize_t d4_file_write_intervals(d4_file_t* handle, const d4_interval_t* buf, size_t count);
int d4_file_tell(const d4_file_t* handle, char* name_buf, size_t buf_size, uint32_t* pos_buf);
int d4_file_seek(d4_file_t* handle, const char* chrom, uint32_t pos);

// The parallel API
typedef struct d4_task_part_t d4_task_part_t;
typedef enum {
    D4_TASK_READ,
    D4_TASK_WRITE,
} d4_task_mode_t;

typedef struct {
    void* task_context;
    int status;
} d4_task_part_result_t;

typedef struct {
    d4_task_mode_t mode;
    uint32_t part_size_limit;
    uint32_t num_cpus;
    void* (*part_context_create_cb)(d4_task_part_t* handle, void* extra_data);
    int (*part_process_cb)(d4_task_part_t* handle, void* task_context, void* extra_data);
    int (*part_finalize_cb)(d4_task_part_result_t* tasks, size_t count,  void* extra_data);
    void* extra_data;
} d4_task_desc_t;
int d4_file_run_task(d4_file_t* handle, d4_task_desc_t* task);
ssize_t d4_task_read_values(d4_task_part_t* task, uint32_t offset, int32_t* buffer, size_t count);
ssize_t d4_task_write_values(d4_task_part_t* task, uint32_t offset, int32_t const* data, size_t count);
ssize_t d4_task_read_intervals(d4_task_part_t* task, d4_interval_t* data, size_t count);
int d4_task_chrom(const d4_task_part_t* task, char* name_buf, size_t name_buf_size);
int d4_task_range(const d4_task_part_t* task, uint32_t* left_buf, uint32_t* right_buf);

// The highlevel API
int d4_file_profile_depth_from_bam(const char* bam_path, const char* d4_path, const d4_file_metadata_t* header);

// Error handling
void d4_error_clear(void);
const char* d4_error_message(char* buf, size_t size);
int d4_error_num(void);
#ifdef __cplusplus
}
#endif
#endif 
