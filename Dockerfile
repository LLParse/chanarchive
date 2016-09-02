FROM alpine:3.3
ADD streamingchan /usr/bin
ENTRYPOINT ["/usr/bin/streamingchan"]