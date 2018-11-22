DFS Master (Name) server
===

Core of DFS. Client uses master's HTTP API for performing FS calls.

Written by Kotlin. Uses Spring Boot, Spring MVC.

## Running DFS
```sh
docker run -ti --rm -p 10010:10010 -v /tmp/storage:/var/storage --net=host quckly/sne-dfs-storage:latest \
    -mhost 127.0.0.1 -mport 10001 -port 10010 -file /var/storage/s1 -size 16KB -interval 5 \
    -guid b2a1172b-fdac-4ec0-9f21-6de789c3fce1
    
docker run -ti --rm -p 10011:10011 -v /tmp/storage:/var/storage --net=host quckly/sne-dfs-storage:latest \
    -mhost 127.0.0.1 -mport 10001 -port 10011 -file /var/storage/s2 -size 16KB -interval 5 \
    -guid 1b4dbacd-0a38-4636-b3ed-101f34a32153
```

## Master server
### Build
```sh
git clone https://github.com/quckly/sne-dfs-master
cd sne-dfs-master
docker build -t quckly/sne-dfs-master:latest .
```

### Run
```sh

```

##### Application params:
- `app....` - ...

## Storage server
### Build
```sh
git clone https://github.com/wavvs/dfstorage sne-dfs-storage
cd sne-dfs-storage
docker build -t quckly/sne-dfs-storage:latest .
```

### Run
```sh

```

##### Application params:
- `app....` - ...

## Extra
##### Guids
https://www.guidgenerator.com/online-guid-generator.aspx
```
26f4517d-b29a-4d53-b8aa-cdc3b0e3051c
c88b5b65-9581-439c-bd2e-0406d35277f1
aca86d79-b268-4888-b55d-e4155605045b
38576f99-9c60-4d74-b0f1-99b119bed48b
a7cba319-4b88-4fc4-9260-5fab53ab4916
9f3dd18b-e7b3-4059-a523-a4c093644452
27c0fc3d-5b49-490b-924b-26010af256b4
f54a6c89-0ff5-4b4f-a479-dc593967cdf1
f23cd248-8fa6-4708-bf93-b3e831a4c2b5
6af7beea-47ab-4534-ab1b-d9f3a3f27420
d1d2e4b5-e6ab-4e0c-82f7-ddfcbb112594
0f12c215-459c-445e-8856-a29a3c1d4100
a18e4e20-cb91-4acd-a344-61115d81eb11
a16d6c1a-d316-43bf-a358-76722d963a9f
a8ee1c53-4601-4fb7-a34f-512ee54c7748
746cc2c5-bcc1-4058-9d5e-d2f4446359ac
45b7907e-54d1-4407-8a3a-3fc3dfa069de
4471d347-e605-4229-accd-bcc0993bb097
e7bfce86-b72e-4161-8c3d-83aa111e8431
```

## Links:
Master server: https://github.com/quckly/sne-dfs-master 
Client application: https://github.com/quckly/sne-dfs-client 
Storage server: https://github.com/wavvs/dfstorage 
