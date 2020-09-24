#ifdef USE_SYSTEM_HTSLIB
#	include <htslib/sam.h>
#	define USE_SYSTEM_HTSLIB 1
#else
#	include <sam.h>
#	define USE_SYSTEM_HTSLIB 0
#endif
