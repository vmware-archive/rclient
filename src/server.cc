/*------------------------------------------------------------------------------
 *
 * Copyright (c) 2016-Present Pivotal Software, Inc
 *
 *------------------------------------------------------------------------------
 */
#include <cassert>
#include <cstring>

#include "rserver.hh"

int main(int argc, char **argv) {
    RServer *server = nullptr;
    RServerLog *log = nullptr;
    // Make sure we have the same length in GPDB and server side
    assert(sizeof(float) == 4 && sizeof(double) == 8);

    // TODO: will we still need catch signal SIGGIV?
    // set_signal_handlers();

    // Initialize Server
    try {
        if (argc <= 2) {
            log = new RServerLog(RServerWorkingMode::CONTAINER, std::string(""));
            server = new RServer(false, log);
            server->initServer();
        } else {

            std::string mode = std::string(argv[1]);

            if (mode.compare("1") == 0) {
                log = new RServerLog(RServerWorkingMode::STANDALONG, std::string(""));
                server = new RServer(true, log);
                log->log(RServerLogLevel::LOGS, "Server start in stand alone mode in %s", argv[1]);
                server->initServer(std::string(argv[2]));
            } else if (mode.compare("2") == 0) {
                log = new RServerLog(RServerWorkingMode::CONTAINER, std::string(""));
                server = new RServer(false, log);
                log->log(RServerLogLevel::LOGS, "Server start in container mode in %s", argv[1]);
                server->initServer();
            } else if (mode.compare("3") == 0) {
                log = new RServerLog(RServerWorkingMode::CONTAINERDEBUG, std::string(argv[2]));
                server = new RServer(false, log);
                log->log(RServerLogLevel::LOGS,
                         "Server start in container debug mode in %s, output file is %s", argv[1],
                         argv[2]);
                server->initServer();
            } else {
                log = new RServerLog(RServerWorkingMode::CONTAINER, std::string(""));
                server = new RServer(false, log);
                log->log(RServerLogLevel::LOGS,
                         "Server start in unknown mode in %s, using default mode", argv[1]);
                server->initServer();
            }
        }

        server->startServer();
    }
    catch (std::exception &e) {
        if (log != nullptr) {
            log->log(RServerLogLevel::LOGS, "Unexpected R server runtime exception, exit now!");
        }
        return -1;
    }
    if (log != nullptr) {
        log->log(RServerLogLevel::LOGS, "Server has finished execution");
    }
    return 0;
}
