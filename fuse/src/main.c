#include "fops.hpp"

static FUSE_OP_ASSIGN(pico_oper);

int main(int argc, char *argv[])
{
	return fuse_main(argc, argv, &pico_oper, NULL);
}
