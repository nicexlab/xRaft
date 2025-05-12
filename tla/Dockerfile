FROM ubuntu:latest

# 安装必要的软件包
RUN apt-get update && apt-get install -y \
    default-jdk \
    git \
    make \
    curl

# 克隆TLA+ bin仓库
RUN git clone https://github.com/pmer/tla-bin.git tla-bin

# 复制xraft tla spec
COPY  ./xraft_spec xraft_spec

# 编译TLA+ Tools
RUN cd tla-bin \
    && ./download_or_update_tla.sh \
    && ./install.sh
