#include "rserver.hh"
#include "gtest/gtest.h"

TEST(RServerTest, RServerInit) {
    RServerLog *log = nullptr;
    log = new RServerLog(RServerWorkingMode::STANDALONG, std::string(""));
    RServer *server = new RServer(RServerWorkingMode::STANDALONG, log);
    (void)server;
}