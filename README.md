# CSE-lab 测试及使用说明

## LAB1: CHFS

1. chfs基于fuse实现，所以需要有fuse对应的库、头文件等内容。实验室提供了装有实验环境的docker容器，运行下列两条命令即可：

```shell
docker pull shenjiahuan/cselab_env:1.0
sudo docker run -it --rm --privileged --cap-add=ALL -v cse-lab文件夹绝对路径:/home/stu/cse-lab shenjiahuan/cselab_env:1.0 /bin/bash
```

2. 在docker下直接到cse-lab文件夹中，执行以下命令即可进行测试

```shell
make clean
make
./grade.sh
```

## LAB2: MapReduce in Word Count

```shell
git checkout lab2
make clean
make 
./grade.sh（可以通过改脚本跳过lab1的测试）
```

## LAB3: Fault Tolerance with raft

```shell
git checkout lab3
make clean
make 
./grade.sh（可以通过改脚本跳过lab1、2的测试）
```

## LAB4: Shared Transactional KVS Service

```shell
git checkout lab4
make clean
make 
./grade.sh（可以通过改脚本跳过lab1、2的测试）
```