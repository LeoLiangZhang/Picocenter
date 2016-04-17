#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>

#define UDP_PORT 2345
#define PAGE_SIZE 4096
#define MEM_SIZE (PAGE_SIZE*256)
#define MAXBUF 1024
// #define TRACE

typedef struct
{
  int sock_fd;
  int port;
  void *mem_addr;
  int mem_size;
  void *last_working_page;
  int pages;
  int working_pages;
  int offset_working_pages;  // set number of pages to move for working pages
} pageloop;

void init_vma(pageloop *pl)
{
  int c;
  char *start, *end;
  pl->mem_addr = mmap(NULL, pl->mem_size, PROT_READ | PROT_WRITE, 
                      MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
  if (pl->mem_addr == MAP_FAILED) {
    perror("Unable to acquire mmap");
    exit(EXIT_FAILURE);
  }
  pl->last_working_page = pl->mem_addr;

  c = 0;
  start = (char*)pl->mem_addr;
  end = start + pl->mem_size;
  while (start++ < end) {
    *start = c++;
  }

  printf("Allocate %d pages of %d bytes VMA at [%p, %p] \n", pl->pages,
    pl->mem_size, pl->mem_addr, ((char*)pl->mem_addr)+pl->mem_size);
}

void update_pages_once(pageloop *pl)
{
  int i;
  char *addr = (char*)pl->last_working_page;
  char *end_addr = ((char*)pl->mem_addr) + pl->mem_size;
  addr = ((char*)pl->mem_addr) + 
          ((((addr - ((char*)pl->mem_addr)) / PAGE_SIZE) + pl->offset_working_pages) % pl->pages) 
          * PAGE_SIZE;
  pl->last_working_page = addr;
  for (i = 0; i < pl->working_pages; i++) {
    *addr += 1; // touch the page
#ifdef TRACE
    printf("Touch page %p\n", addr);
#endif
    addr += PAGE_SIZE;
    if (addr >= end_addr) {
      addr = (char*)pl->mem_addr;
    }
  }
}

void init_udp_server(pageloop *pl)
{
  struct sockaddr_in skaddr;
  socklen_t length;

  if ((pl->sock_fd = socket( PF_INET, SOCK_DGRAM, 0 )) < 0) {
    perror("Problem creating udp listening socket\n");
    exit(EXIT_FAILURE);
  }

  skaddr.sin_family = AF_INET;
  skaddr.sin_addr.s_addr = htonl(INADDR_ANY);
  skaddr.sin_port = htons(pl->port);

  if (bind(pl->sock_fd, (struct sockaddr *) &skaddr, sizeof(skaddr)) < 0) {
    perror("Problem binding\n");
    exit(EXIT_FAILURE);
  }

  length = sizeof( skaddr );
  if (getsockname(pl->sock_fd, (struct sockaddr *) &skaddr, &length) < 0) {
    perror("Error getsockname\n");
    exit(EXIT_FAILURE);
  }

  printf("The server UDP port number is %d\n", ntohs(skaddr.sin_port));
}

int serve_one_request(pageloop *pl) {
  int n;
  socklen_t len;
  char bufin[MAXBUF];
  struct sockaddr_in remote;
  int sd = pl->sock_fd;

  /* need to know how big address struct is, len must be set before the
     call to recvfrom!!! */

  len = sizeof(remote);

  /* read a datagram from the socket (put result in bufin) */
  n=recvfrom(sd,bufin,MAXBUF,0,(struct sockaddr *)&remote,&len);

  /* print out the address of the sender */
#ifdef TRACE
  printf("Got a datagram of %d bytes from %s port %d\n", n,
         inet_ntoa(remote.sin_addr), ntohs(remote.sin_port));
#endif

  // do some work
  update_pages_once(pl);

  if (n<0) {
    perror("Error receiving data");
    return 0;
  } else {
    // printf("GOT %d BYTES\n",n);
    /* Got something, just send it back */
    sendto(sd,bufin,n,0,(struct sockaddr *)&remote,len);
  }
  return 1;
}

int main(int argc, char const *argv[])
{
  pageloop pl;
  // init pl
  pl.port = UDP_PORT;
  pl.pages = 256;               // 256 pages * 4K = 1M of mmap area
  pl.working_pages = 1;         // only touch one page per request
  pl.offset_working_pages = 0;  // don't move working set

  if (argc > 1) {
    pl.pages = atoi(argv[1]);
  }
  if (argc > 2) {
    pl.working_pages = atoi(argv[2]);
  }
  if (argc > 3) {
    pl.offset_working_pages = atoi(argv[3]);
  } 
  if (argc > 4) {
    printf("usage: %s [total_pages] [working_pages] [offset_working_pages]", argv[0]);
    exit(EXIT_FAILURE);
  }
  pl.mem_size = pl.pages * PAGE_SIZE;
  
  init_udp_server(&pl);
  init_vma(&pl);

  while (serve_one_request(&pl));

  return EXIT_SUCCESS;
}
