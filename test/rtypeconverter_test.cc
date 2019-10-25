#include "rcall.hh"
#include "rtypeconverter.cc"
#include "gtest/gtest.h"
#include "plcontainer.pb.h"
#include "plcontainer.grpc.pb.h"

using namespace plcontainer;

class RConvTest : public testing::Test {
   protected:
    virtual void SetUp() { core = new RCoreRuntime(); }
    virtual void TearDown() { delete core; }

    RCoreRuntime *core;
};

TEST_F(RConvTest, RConvResultsREAL) {
    CallRequest *request = new CallRequest();
    CallResponse *response = new CallResponse();

    ProcSrc *src = request->mutable_proc();

    src->set_name("test");
    src->set_src("return (1.2)");
    request->set_rettype(PlcDataType::REAL);

    EXPECT_EQ(ReturnStatus::OK, core->prepare(request));
    EXPECT_EQ(ReturnStatus::OK, core->execute());
    EXPECT_EQ(ReturnStatus::OK, core->getResults(response));
    EXPECT_EQ(1.2, response->results()[0].scalarvalue().realvalue());
    EXPECT_EQ(PlcDataType::REAL, response->results()[0].type());
}

TEST_F(RConvTest, RConvResultsLOGICAL) {
    CallRequest *request = new CallRequest();
    CallResponse *response = new CallResponse();

    ProcSrc *src = request->mutable_proc();

    src->set_name("test");
    src->set_src("return (FALSE)");
    request->set_rettype(PlcDataType::LOGICAL);

    EXPECT_EQ(ReturnStatus::OK, core->prepare(request));
    EXPECT_EQ(ReturnStatus::OK, core->execute());
    EXPECT_EQ(ReturnStatus::OK, core->getResults(response));
    EXPECT_EQ(false, response->results()[0].scalarvalue().logicalvalue());
    EXPECT_EQ(PlcDataType::LOGICAL, response->results()[0].type());
}

TEST_F(RConvTest, RConvResultsINT) {
    CallRequest *request = new CallRequest();
    CallResponse *response = new CallResponse();

    ProcSrc *src = request->mutable_proc();

    src->set_name("test");
    src->set_src("return (10000)");
    request->set_rettype(PlcDataType::INT);

    EXPECT_EQ(ReturnStatus::OK, core->prepare(request));
    EXPECT_EQ(ReturnStatus::OK, core->execute());
    EXPECT_EQ(ReturnStatus::OK, core->getResults(response));
    EXPECT_EQ(10000, response->results()[0].scalarvalue().intvalue());
    EXPECT_EQ(PlcDataType::INT, response->results()[0].type());
}

TEST_F(RConvTest, RConvResultsTEXT) {
    CallRequest *request = new CallRequest();
    CallResponse *response = new CallResponse();

    ProcSrc *src = request->mutable_proc();

    src->set_name("test");
    src->set_src("return (\"abc\")");
    request->set_rettype(PlcDataType::TEXT);

    EXPECT_EQ(ReturnStatus::OK, core->prepare(request));
    EXPECT_EQ(ReturnStatus::OK, core->execute());
    EXPECT_EQ(ReturnStatus::OK, core->getResults(response));
    EXPECT_EQ("abc", response->results()[0].scalarvalue().stringvalue());
    EXPECT_EQ(PlcDataType::TEXT, response->results()[0].type());
}

TEST_F(RConvTest, RConvArgLOGICAL) {
    CallRequest *request = new CallRequest();
    CallResponse *response = new CallResponse();

    PlcValue *arg = request->add_args();
    ScalarData *data;
    ProcSrc *src = request->mutable_proc();

    request->set_rettype(PlcDataType::LOGICAL);

    src->set_name("test");
    src->set_src("return (a)");
    arg->set_name("a");
    arg->set_type(PlcDataType::LOGICAL);
    data = arg->mutable_scalarvalue();
    data->set_logicalvalue(false);

    EXPECT_EQ(ReturnStatus::OK, core->prepare(request));
    EXPECT_EQ(ReturnStatus::OK, core->execute());
    EXPECT_EQ(ReturnStatus::OK, core->getResults(response));
    EXPECT_NO_THROW(core->cleanup());
    EXPECT_EQ(false, response->results()[0].scalarvalue().logicalvalue());
    EXPECT_EQ(PlcDataType::LOGICAL, response->results()[0].type());
}

TEST_F(RConvTest, RConvArgINT) {
    CallRequest *request = new CallRequest();
    CallResponse *response = new CallResponse();

    PlcValue *arg = request->add_args();
    ScalarData *data;
    ProcSrc *src = request->mutable_proc();

    request->set_rettype(PlcDataType::INT);

    src->set_name("test");
    src->set_src("return (b)");
    arg->set_name("b");
    arg->set_type(PlcDataType::INT);
    data = arg->mutable_scalarvalue();
    data->set_intvalue(100);

    EXPECT_EQ(ReturnStatus::OK, core->prepare(request));
    EXPECT_EQ(ReturnStatus::OK, core->execute());
    EXPECT_EQ(ReturnStatus::OK, core->getResults(response));
    EXPECT_NO_THROW(core->cleanup());
    EXPECT_EQ(100, response->results()[0].scalarvalue().intvalue());
    EXPECT_EQ(PlcDataType::INT, response->results()[0].type());
}

TEST_F(RConvTest, RConvArgREAL) {
    CallRequest *request = new CallRequest();
    CallResponse *response = new CallResponse();

    PlcValue *arg = request->add_args();
    ScalarData *data;
    ProcSrc *src = request->mutable_proc();

    request->set_rettype(PlcDataType::REAL);

    src->set_name("test");
    src->set_src("return (a)");
    arg->set_name("a");
    arg->set_type(PlcDataType::REAL);
    data = arg->mutable_scalarvalue();
    data->set_realvalue(1.2);

    EXPECT_EQ(ReturnStatus::OK, core->prepare(request));
    EXPECT_EQ(ReturnStatus::OK, core->execute());
    EXPECT_EQ(ReturnStatus::OK, core->getResults(response));
    EXPECT_NO_THROW(core->cleanup());
    EXPECT_EQ(1.2, response->results()[0].scalarvalue().realvalue());
    EXPECT_EQ(PlcDataType::REAL, response->results()[0].type());
}

TEST_F(RConvTest, RConvArgTEXT) {
    CallRequest *request = new CallRequest();
    CallResponse *response = new CallResponse();

    PlcValue *arg = request->add_args();
    ScalarData *data;
    ProcSrc *src = request->mutable_proc();

    request->set_rettype(PlcDataType::TEXT);

    src->set_name("test");
    src->set_src("return (a)");
    arg->set_name("a");
    arg->set_type(PlcDataType::TEXT);
    data = arg->mutable_scalarvalue();
    data->set_stringvalue("abc");

    EXPECT_EQ(ReturnStatus::OK, core->prepare(request));
    EXPECT_EQ(ReturnStatus::OK, core->execute());
    EXPECT_EQ(ReturnStatus::OK, core->getResults(response));
    EXPECT_NO_THROW(core->cleanup());
    EXPECT_EQ("abc", response->results()[0].scalarvalue().stringvalue());
    EXPECT_EQ(PlcDataType::TEXT, response->results()[0].type());
}