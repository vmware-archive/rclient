#include "rutils.hh"
#include "gtest/gtest.h"

class RLogTest : public testing::Test {
   protected:
    virtual void SetUp() { log = new RServerLog(RServerWorkingMode::CONTAINER, std::string("")); }
    virtual void TearDown() { delete log; }

    RServerLog *log;
};

TEST(RUtilsTest, RLogInit) {
    RServerLog *log = nullptr;
    log = new RServerLog(RServerWorkingMode::STANDALONG, std::string(""));
    delete log;
    log = new RServerLog(RServerWorkingMode::CONTAINER, std::string(""));
    delete log;
    log = new RServerLog(RServerWorkingMode::CONTAINERDEBUG, std::string("/tmp/rserver_log.log"));
}

TEST_F(RLogTest, RLogErrorException) {
    EXPECT_THROW(log->log(RServerLogLevel::ERRORS, "test error"), RServerErrorException);
}

TEST_F(RLogTest, RLogWarningException) {
    EXPECT_THROW(log->log(RServerLogLevel::WARNINGS, "test warning"), RServerWarningException);
}
