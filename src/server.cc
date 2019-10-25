/*------------------------------------------------------------------------------
 *
 * Copyright (c) 2016-Present Pivotal Software, Inc
 *
 *------------------------------------------------------------------------------
 */
#include <cassert>
#include <cstring>

#include "rserver.hh"

int main(int argc, char** argv) {
    RServer* server;
    RServerLog log;
    // Make sure we have the same length in GPDB and server side
    assert(sizeof(float) == 4 && sizeof(double) == 8);

    // TODO: will we still need catch signal SIGGIV?
    // set_signal_handlers();

    // Initialize Server
    log.setLogLevel(RServerLogLevel::LOGS);
    if (argc <= 1) {
        server = new RServer(false);
        server->initServer();
    } else {
        server = new RServer(true);
        log.log(RServerLogLevel::LOGS, "Server start in stand alone mode in %s", argv[1]);
        server->initServer(std::string(argv[1]));
    }

    server->startServer();
    log.log(RServerLogLevel::LOGS, "Server has finished execution");
    return 0;
}
