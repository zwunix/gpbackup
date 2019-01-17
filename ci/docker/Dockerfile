FROM pivotaldata/centos-gpdb-dev:6-gcc6.2-llvm3.7

ENV GOPATH=/go
ENV PATH=$GOPATH/bin:${PATH}

RUN go get github.com/golang/dep/cmd/dep github.com/alecthomas/gometalinter && \
    gometalinter --install && \
	curl https://raw.githubusercontent.com/golang/dep/master/install.sh | sh

COPY . $GOPATH/src/github.com/greenplum-db/gpbackup/

WORKDIR $GOPATH/src/github.com/greenplum-db/gpbackup/
RUN cd $GOPATH/src/github.com/greenplum-db/gpbackup/ && \
    dep ensure && \
    cd $GOPATH/src/github.com/greenplum-db/gpbackup/vendor/github.com/onsi/ginkgo/ginkgo && \
    go install . && \
    cd $GOPATH/src/github.com/greenplum-db/gpbackup/vendor/golang.org/x/tools/cmd/goimports && \
    go install .


# remove all gpbackup source, leaving just vendor directory
RUN mkdir /tmp/empty && \
    rsync -r --delete /tmp/empty/ $GOPATH/src/github.com/greenplum-db/gpbackup/ --exclude vendor
