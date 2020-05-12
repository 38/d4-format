#include <d4.h>

int main() 
{
	char buf[1024];
	printf("%d %s\n", d4_error_num(), d4_error_message(buf, 1024));
	d4_file_t* fp = d4_open("/","w");
	printf("%d %s\n", d4_error_num(), d4_error_message(buf, 1024));
	fp = d4_open(NULL, NULL);
	printf("%d %s\n", d4_error_num(), d4_error_message(buf, 1024));
	d4_error_clear();
	printf("%d %s\n", d4_error_num(), d4_error_message(buf, 1024));
	return 0;
}
