FROM alpine:3.3
ADD chanarchive /usr/bin
ENTRYPOINT ["/usr/bin/chanarchive"]