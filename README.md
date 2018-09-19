## Features

    + maintains persistent upstream connections
    + keeps low connection count on upstream servers
    + deals with upstream failures
    + supports SET, GET, SETEX, DEL, INCR, DECR, EXPIRE, EXISTS commands

## Build

install jruby (tested with 9.1), then:

    $ bundle install
    $ warble compiled jar
    
## Configure

create config.yml with the following content

    log_level: 'debug'

    server:
        -
            type: 'cluster'
            listen: '127.0.0.1:26379'
            buffer: 16384
            timeout: 1
            upstream:
                - '127.0.0.1:6379'


## Run

    $ java -jar redis-proxy.jar
