#include "rserver.cc"
#include "gtest/gtest.h"

TEST(RServerTest, RServerInit) {
    RServer *server = new RServer(true);
    (void)server;
}