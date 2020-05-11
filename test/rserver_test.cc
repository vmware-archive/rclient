#include "rserver.hh"
#include "gtest/gtest.h"

TEST(RServerTest, RServerInit) {
    RServerLog *log = nullptr;
    log = new RServerLog(RServerWorkingMode::STANDALONG, std::string(""));
    RServer *server = new RServer(RServerWorkingMode::STANDALONG, log);
    (void)server;
}

TEST(RServerTest, RServerPL4KModeInit) {
    RServerLog *log = nullptr;
    log = new RServerLog(RServerWorkingMode::PL4KDEBUG, std::string(""));
    RServer *server = new RServer(RServerWorkingMode::PL4KDEBUG, log);
    // only need init once
    server->initServer("localhost", "8000");
}

TEST(RServerTest, RServerPL4KModeRunSingle) {
    RServerLog *log = nullptr;
    log = new RServerLog(RServerWorkingMode::PL4KDEBUG, std::string(""));
    RServerRPC *rpc = new RServerRPC(RServerWorkingMode::PL4KDEBUG, log);

    // Full test
    plcontainer::CallRequest *request = new plcontainer::CallRequest();
    plcontainer::CallResponse *response = new plcontainer::CallResponse();
    ServerContext *context = new ServerContext();

    ProcSrc *src = request->mutable_proc();
    ReturnType *ret = request->mutable_rettype();

    src->set_name("test");
    src->set_src("return (1)");
    ret->set_type(PlcDataType::INT);

    rpc->FunctionCall(context, request, response);
    EXPECT_EQ(1, response->results()[0].scalarvalue().intvalue());
    EXPECT_EQ(PlcDataType::INT, response->results()[0].type());
}

TEST(RServerTest, RServerPL4KModeRunTwoSession) {
    RServerLog *log = nullptr;
    log = new RServerLog(RServerWorkingMode::PL4KDEBUG, std::string(""));
    RServerRPC *rpc1 = new RServerRPC(RServerWorkingMode::PL4KDEBUG, log);
    RServerRPC *rpc2 = new RServerRPC(RServerWorkingMode::PL4KDEBUG, log);

    // Full test
    plcontainer::CallRequest *request = new plcontainer::CallRequest();
    plcontainer::CallResponse *response1 = new plcontainer::CallResponse();
    plcontainer::CallResponse *response2 = new plcontainer::CallResponse();

    ServerContext *context = new ServerContext();

    ProcSrc *src = request->mutable_proc();
    ReturnType *ret = request->mutable_rettype();

    src->set_name("test");
    src->set_src("return (1)");
    ret->set_type(PlcDataType::INT);

    rpc1->FunctionCall(context, request, response1);
    rpc2->FunctionCall(context, request, response2);

    EXPECT_EQ(1, response1->results()[0].scalarvalue().intvalue());
    EXPECT_EQ(PlcDataType::INT, response1->results()[0].type());

    EXPECT_EQ(1, response2->results()[0].scalarvalue().intvalue());
    EXPECT_EQ(PlcDataType::INT, response2->results()[0].type());
}

TEST(RServerTest, RServerPL4KModeRunThreeSession) {
    RServerLog *log = nullptr;
    log = new RServerLog(RServerWorkingMode::PL4KDEBUG, std::string(""));
    RServerRPC *rpc1 = new RServerRPC(RServerWorkingMode::PL4KDEBUG, log);
    RServerRPC *rpc2 = new RServerRPC(RServerWorkingMode::PL4KDEBUG, log);
    RServerRPC *rpc3 = new RServerRPC(RServerWorkingMode::PL4KDEBUG, log);

    // Full test
    plcontainer::CallRequest *request = new plcontainer::CallRequest();
    plcontainer::CallResponse *response1 = new plcontainer::CallResponse();
    plcontainer::CallResponse *response2 = new plcontainer::CallResponse();
    plcontainer::CallResponse *response3 = new plcontainer::CallResponse();

    ServerContext *context = new ServerContext();

    ProcSrc *src = request->mutable_proc();
    ReturnType *ret = request->mutable_rettype();

    src->set_name("test");
    src->set_src("return (1)");
    ret->set_type(PlcDataType::INT);

    rpc1->FunctionCall(context, request, response1);
    rpc2->FunctionCall(context, request, response2);
    rpc3->FunctionCall(context, request, response3);

    EXPECT_EQ(1, response1->results()[0].scalarvalue().intvalue());
    EXPECT_EQ(PlcDataType::INT, response1->results()[0].type());

    EXPECT_EQ(1, response2->results()[0].scalarvalue().intvalue());
    EXPECT_EQ(PlcDataType::INT, response2->results()[0].type());

    EXPECT_EQ(1, response3->results()[0].scalarvalue().intvalue());
    EXPECT_EQ(PlcDataType::INT, response3->results()[0].type());
}
