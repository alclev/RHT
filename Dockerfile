FROM rockylinux:8

RUN dnf install -y \
    gcc-toolset-13 \
    make \
    && dnf clean all

ENV PATH=/opt/rh/gcc-toolset-13/root/usr/bin:$PATH

WORKDIR /root
COPY . .
ENV MODE=debug
CMD ["bash", "-c", "make clean; make -j$(nproc) ${MODE}"]
# sudo docker run -e MODE=debug --privileged --rm -v $(pwd):/root --name mu -it rht:latest
