#include "rcall.hh"
#include "gtest/gtest.h"
#include "plcontainer.grpc.pb.h"
#include "plcontainer.pb.h"

using namespace plcontainer;

class RCallTest : public testing::Test {
   protected:
    virtual void SetUp() {
        logs = new RServerLog(RServerWorkingMode::CONTAINER, std::string(""));
        core = new RCoreRuntime(logs);
    }

    virtual void TearDown() {
        delete core;
        delete logs;
    }

    RCoreRuntime *core;
    RServerLog *logs;
};

// TEST_F(RCallTest, RCoreInit) { EXPECT_EQ(ReturnStatus::OK, core->init()); }

TEST_F(RCallTest, RCorePrePareNoArgument) {
    plcontainer::CallRequest *request = new plcontainer::CallRequest();
    ProcSrc *src = request->mutable_proc();
    src->set_name("test");
    src->set_src("return (1)");
    EXPECT_EQ(ReturnStatus::OK, core->prepare(request));
}

TEST_F(RCallTest, RCorePrePareWithOneArgument) {
    plcontainer::CallRequest *request = new plcontainer::CallRequest();
    PlcValue *arg = request->add_args();
    ScalarData *data;
    ProcSrc *src = request->mutable_proc();

    src->set_name("test");
    src->set_src("return (a)");
    arg->set_name("a");
    arg->set_type(PlcDataType::INT);
    data = arg->mutable_scalarvalue();
    data->set_intvalue(2);

    EXPECT_EQ(ReturnStatus::OK, core->prepare(request));
}

TEST_F(RCallTest, RCorePrePareWithTwoArgument) {
    plcontainer::CallRequest *request = new plcontainer::CallRequest();
    PlcValue *arg1 = request->add_args();
    PlcValue *arg2 = request->add_args();
    ScalarData *data1;
    ScalarData *data2;
    ProcSrc *src = request->mutable_proc();

    src->set_name("test");
    src->set_src("return (a)");
    arg1->set_name("a");
    arg1->set_type(PlcDataType::INT);
    data1 = arg1->mutable_scalarvalue();
    data1->set_intvalue(2);

    arg2->set_name("b");
    arg2->set_type(PlcDataType::REAL);
    data2 = arg2->mutable_scalarvalue();
    data2->set_realvalue(2.0);

    EXPECT_EQ(ReturnStatus::OK, core->prepare(request));
}

TEST_F(RCallTest, RCoreEXECWithNoArgument) {
    plcontainer::CallRequest *request = new plcontainer::CallRequest();
    ProcSrc *src = request->mutable_proc();

    src->set_name("test");
    src->set_src("return (1)");

    EXPECT_EQ(ReturnStatus::OK, core->prepare(request));
    EXPECT_EQ(ReturnStatus::OK, core->execute());
}

TEST_F(RCallTest, RCoreEXECWithOneArgument) {
    plcontainer::CallRequest *request = new plcontainer::CallRequest();
    PlcValue *arg = request->add_args();
    ScalarData *data;
    ProcSrc *src = request->mutable_proc();

    src->set_name("test");
    src->set_src("return (a)");
    arg->set_name("a");
    arg->set_type(PlcDataType::INT);
    data = arg->mutable_scalarvalue();
    data->set_intvalue(2);

    EXPECT_EQ(ReturnStatus::OK, core->prepare(request));
    EXPECT_EQ(ReturnStatus::OK, core->execute());
}

TEST_F(RCallTest, RCoreEXECWithNoArgumentAndReturnHasArgument) {
    plcontainer::CallRequest *request = new plcontainer::CallRequest();
    ProcSrc *src = request->mutable_proc();

    src->set_name("test");
    src->set_src("return (a)");

    EXPECT_EQ(ReturnStatus::OK, core->prepare(request));
    EXPECT_THROW(core->execute(), RServerErrorException);
}

TEST_F(RCallTest, RCoreGetResultsWithNoArgumentINT) {
    plcontainer::CallRequest *request = new plcontainer::CallRequest();
    plcontainer::CallResponse *response = new plcontainer::CallResponse();

    ProcSrc *src = request->mutable_proc();
    ReturnType *ret = request->mutable_rettype();

    src->set_name("test");
    src->set_src("return (1)");
    ret->set_type(PlcDataType::INT);

    EXPECT_EQ(ReturnStatus::OK, core->prepare(request));
    EXPECT_EQ(ReturnStatus::OK, core->execute());
    EXPECT_EQ(ReturnStatus::OK, core->getResults(response));
    EXPECT_EQ(1, response->results()[0].scalarvalue().intvalue());
    EXPECT_EQ(PlcDataType::INT, response->results()[0].type());
}

TEST_F(RCallTest, RCoreGetResultsWithVoidReturnType) {
    plcontainer::CallRequest *request = new plcontainer::CallRequest();
    plcontainer::CallResponse *response = new plcontainer::CallResponse();

    ProcSrc *src = request->mutable_proc();
    ReturnType *ret = request->mutable_rettype();

    src->set_name("test");
    src->set_src("return (1)");
    ret->set_type(PlcDataType::VOID);

    EXPECT_EQ(ReturnStatus::OK, core->prepare(request));
    EXPECT_EQ(ReturnStatus::OK, core->execute());
    EXPECT_EQ(ReturnStatus::OK, core->getResults(response));
    EXPECT_EQ(PlcDataType::VOID, response->results()[0].type());
}
