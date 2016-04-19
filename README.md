# [Picocenter](http://leoliangzhang.github.io/Picocenter/)

Picocenter is the first attempt to run LLMI applications in today's cloud environments efficiently. Picocenter explores an alternative approach for cloud computation based on a process-like abstraction rather than a virtual machine abstraction, thereby gaining the scalability and efficiency of PaaS along with the generality of IaaS. Picocenter uses Linux container (LXC) to provide a hosting infrastructure that enables use of legacy applications.

In order to run LLMI applications efficiently in the cloud, Picocenter leverages LLMI workload by swapping idle applications to cloud storage (since, by definition, applications are largely idle) and swapping them back quickly. To avoid costly delays when restoring applications, Picocenter develops an ActiveSet technique that learns the set of commonly accessed memory pages when requests arrive; Picocenter first prefetches those pages from cloud storage before fetching additional pages on-demand. ActiveSet technique represents a prediction of the memory pages that are likely to be accessed the next time an application is restored.

# Quick start

Clone or download source code from this repo. Assuming your local copy is in /picocenter. 

## FUSE application

First build the FUSE application:

```
cd /picocenter
make 
```

Then start: `/picocenter/fuse/main /checkpoints/ -f`

## Run the hub

```
sudo python hub-heart.py 9997 &
sudo python hub-dns.py 53 &
```

## Run the worker

`sudo python /elasticity/worker/worker.py`

## Install Picocenter CRIU

Picocenter uses a modified CRIU, which can be found at https://github.com/LeoLiangZhang/Picocenter-criu. Our CRIU map applications memory pages to files, which is necessary for obtaining page accessing information that is used to build ActiveSet. Please clone or download the source code, and `make && make install`. 


