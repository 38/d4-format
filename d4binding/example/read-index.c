#include <stdio.h>
#include <d4.h>

int main() {
    d4_file_t* fp = d4_open("https://d4-format-testing.s3.us-west-1.amazonaws.com/hg002.d4", "r");
    
    printf("d4_index_check = %d\n", d4_index_check(fp, D4_INDEX_KIND_SUM));
    d4_index_result_t result;
    d4_index_query(fp, D4_INDEX_KIND_SUM, "1", 0, 10000000, &result);
    printf("mean depth of chr1:0-10MB %lf\n", result.sum / 10000000);

    return 0;
}