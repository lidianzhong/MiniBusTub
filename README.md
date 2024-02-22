CMU 15-445数据库课程项目的简化版本

原仓库地址: [github.com/cmu-db/bustub](https://github.com/cmu-db/bustub)

## 构建

1. 安装 Ubuntu 22.04 系统
2. 安装 git
    ```bash
    sudo apt update
    sudo apt install git
    ```
3. 下载项目源代码
    ```bash
    git clone https://github.com/lidianzhong/MiniBusTub.git
   ```
4. 安装相关依赖
   ```bash
    sudo build_support/packages.sh
   ```
5. 构建项目（在根目录执行命令）
   ```bash
   mkdir build
   cd build
   cmake ..
   make
   ```
6. 运行 shell（在根目录执行命令）
    ```bash
    cd build && make -j$(nproc) shell
    ./bin/bustub-shell
    ```

## 内容

- [x] 完成 LRU-K 缓存算法
- [x] 完成 Disk Scheduler 磁盘调度算法
- [x] 完成 Buffer Pool Manager 缓冲池管理器
- [x] 完成 Extendible Hashing 可扩展哈希
- [ ] 完成以下 Executors 执行器
  - [x] Seq Scan
  - [x] Insert
  - [x] Update
  - [x] Delete
  - [x] Index Scan
  - [x] Aggregate
  - [ ] Hash Join
  - [ ] Nested Loop Join
  - [ ] Sort
  - [ ] Limit
- [x] 完成顺序查找优化为索引查找
- [x] 去除了用不到的代码和第三方库
- [x] 删除了 Concurrency Control 和 Log Manager 相关的代码
- [x] 执行 SQL 语句添加运行时间显示