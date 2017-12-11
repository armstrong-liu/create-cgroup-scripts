此脚本针对超融合架构下，根据物理服务器的资源和配置文件，自动创建合理的cgroup，实现资源的隔离。


实现了两种资源的隔离，CPU和内存。CPU和内存资源的分配，采用“就近”分配原则，即处理器访问它自己的本地存储器的速度比非本地存储器（存储器的地方到另一个处理器之间共享的处理器或存储器）快一些。


注意：
此脚本只适用于两种融合方式：
1. Controller + Compute + Ceph OSD + Mongo
2. Compute + Ceph OSD

对于其他的融合方式，暂时不支持。