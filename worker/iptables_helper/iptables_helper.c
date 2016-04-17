#include <unistd.h>

int main(int argc, char *argv[], char *env[]) {
	const char* iptables_path = "/sbin/iptables";
	argv[0] = (char*)iptables_path;
	execve(iptables_path, argv, env);
	return 0;
}
