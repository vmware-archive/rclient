#include "rtypeconverter.hh"
#include "gtest/gtest.h"
#include "plcontainer.grpc.pb.h"
#include "plcontainer.pb.h"
#include "rcall.hh"

using namespace plcontainer;

class RConvTest : public testing::Test {
   protected:
    virtual void SetUp() {
        logs = new RServerLog(RServerWorkingMode::STANDALONG, std::string(""));
        core = new RCoreRuntime(logs);
    }
    virtual void TearDown() {
        delete core;
        delete logs;
    }

    RCoreRuntime *core;
    RServerLog *logs;
};

TEST_F(RConvTest, RConvResultsREAL) {
    plcontainer::CallRequest *request = new plcontainer::CallRequest();
    plcontainer::CallResponse *response = new plcontainer::CallResponse();

    ProcSrc *src = request->mutable_proc();
    ReturnType *ret = request->mutable_rettype();

    src->set_name("test");
    src->set_src("return (1.2)");
    ret->set_type(PlcDataType::REAL);

    EXPECT_EQ(ReturnStatus::OK, core->prepare(request));
    EXPECT_EQ(ReturnStatus::OK, core->execute());
    EXPECT_EQ(ReturnStatus::OK, core->getResults(response));
    EXPECT_EQ(1.2, response->results()[0].scalarvalue().realvalue());
    EXPECT_EQ(PlcDataType::REAL, response->results()[0].type());
}

TEST_F(RConvTest, RConvResultsLOGICAL) {
    plcontainer::CallRequest *request = new plcontainer::CallRequest();
    plcontainer::CallResponse *response = new plcontainer::CallResponse();

    ProcSrc *src = request->mutable_proc();
    ReturnType *ret = request->mutable_rettype();

    src->set_name("test");
    src->set_src("return (FALSE)");
    ret->set_type(PlcDataType::LOGICAL);

    EXPECT_EQ(ReturnStatus::OK, core->prepare(request));
    EXPECT_EQ(ReturnStatus::OK, core->execute());
    EXPECT_EQ(ReturnStatus::OK, core->getResults(response));
    EXPECT_EQ(false, response->results()[0].scalarvalue().logicalvalue());
    EXPECT_EQ(PlcDataType::LOGICAL, response->results()[0].type());
}

TEST_F(RConvTest, RConvResultsINT) {
    plcontainer::CallRequest *request = new plcontainer::CallRequest();
    plcontainer::CallResponse *response = new plcontainer::CallResponse();

    ProcSrc *src = request->mutable_proc();
    ReturnType *ret = request->mutable_rettype();

    src->set_name("test");
    src->set_src("return (10000)");
    ret->set_type(PlcDataType::INT);

    EXPECT_EQ(ReturnStatus::OK, core->prepare(request));
    EXPECT_EQ(ReturnStatus::OK, core->execute());
    EXPECT_EQ(ReturnStatus::OK, core->getResults(response));
    EXPECT_EQ(10000, response->results()[0].scalarvalue().intvalue());
    EXPECT_EQ(PlcDataType::INT, response->results()[0].type());
}

TEST_F(RConvTest, RConvResultsTEXT) {
    plcontainer::CallRequest *request = new plcontainer::CallRequest();
    plcontainer::CallResponse *response = new plcontainer::CallResponse();

    ProcSrc *src = request->mutable_proc();
    ReturnType *ret = request->mutable_rettype();

    src->set_name("test");
    src->set_src("return (\"abc\")");
    ret->set_type(PlcDataType::TEXT);

    EXPECT_EQ(ReturnStatus::OK, core->prepare(request));
    EXPECT_EQ(ReturnStatus::OK, core->execute());
    EXPECT_EQ(ReturnStatus::OK, core->getResults(response));
    EXPECT_EQ("abc", response->results()[0].scalarvalue().stringvalue());
    EXPECT_EQ(PlcDataType::TEXT, response->results()[0].type());
}

TEST_F(RConvTest, RConvArgLOGICAL) {
    plcontainer::CallRequest *request = new plcontainer::CallRequest();
    plcontainer::CallResponse *response = new plcontainer::CallResponse();

    PlcValue *arg = request->add_args();
    ScalarData *data;
    ProcSrc *src = request->mutable_proc();
    ReturnType *ret = request->mutable_rettype();

    ret->set_type(PlcDataType::LOGICAL);

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
    plcontainer::CallRequest *request = new plcontainer::CallRequest();
    plcontainer::CallResponse *response = new plcontainer::CallResponse();

    PlcValue *arg = request->add_args();
    ScalarData *data;
    ProcSrc *src = request->mutable_proc();
    ReturnType *ret = request->mutable_rettype();

    ret->set_type(PlcDataType::INT);

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
    plcontainer::CallRequest *request = new plcontainer::CallRequest();
    plcontainer::CallResponse *response = new plcontainer::CallResponse();

    PlcValue *arg = request->add_args();
    ScalarData *data;
    ProcSrc *src = request->mutable_proc();
    ReturnType *ret = request->mutable_rettype();

    ret->set_type(PlcDataType::REAL);

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
    plcontainer::CallRequest *request = new plcontainer::CallRequest();
    plcontainer::CallResponse *response = new plcontainer::CallResponse();

    PlcValue *arg = request->add_args();
    ScalarData *data;
    ProcSrc *src = request->mutable_proc();
    ReturnType *ret = request->mutable_rettype();

    ret->set_type(PlcDataType::TEXT);

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

TEST_F(RConvTest, RConvArgUdtWithTwoElementsTestOne) {
    plcontainer::CallRequest *request = new plcontainer::CallRequest();
    plcontainer::CallResponse *response = new plcontainer::CallResponse();
    CompositeData *data;
    ScalarData *subType1;
    ScalarData *subType2;

    PlcValue *arg = request->add_args();

    ProcSrc *src = request->mutable_proc();
    ReturnType *ret = request->mutable_rettype();

    ret->set_type(PlcDataType::INT);

    src->set_name("test");
    src->set_src(
        "print(a)\n if ( (a[1,1] != TRUE) || (typeof(a[1,1]) != 'logical') ) return (0)\n "
        "return(1)");
    arg->set_name("a");
    arg->set_type(PlcDataType::COMPOSITE);
    data = arg->mutable_compositevalue();
    subType1 = data->add_values();
    subType2 = data->add_values();

    subType1->set_type(PlcDataType::LOGICAL);
    subType1->set_logicalvalue(true);
    subType2->set_type(PlcDataType::INT);
    subType2->set_intvalue(2);

    EXPECT_EQ(ReturnStatus::OK, core->prepare(request));
    EXPECT_EQ(ReturnStatus::OK, core->execute());
    EXPECT_EQ(ReturnStatus::OK, core->getResults(response));
    EXPECT_NO_THROW(core->cleanup());
    EXPECT_EQ(1, response->results()[0].scalarvalue().intvalue());
    EXPECT_EQ(PlcDataType::INT, response->results()[0].type());
}

TEST_F(RConvTest, RConvArgUdtWithTwoElementsTestTwo) {
    plcontainer::CallRequest *request = new plcontainer::CallRequest();
    plcontainer::CallResponse *response = new plcontainer::CallResponse();
    CompositeData *data;
    ScalarData *subType1;
    ScalarData *subType2;

    PlcValue *arg = request->add_args();

    ProcSrc *src = request->mutable_proc();
    ReturnType *ret = request->mutable_rettype();

    ret->set_type(PlcDataType::INT);

    src->set_name("test");
    src->set_src(
        "print(a)\n if ( (a[1,2] != 2) || (typeof(a[1,2]) != 'integer') ) return (0)\n "
        "return(1)");
    arg->set_name("a");
    arg->set_type(PlcDataType::COMPOSITE);
    data = arg->mutable_compositevalue();
    subType1 = data->add_values();
    subType2 = data->add_values();

    subType1->set_type(PlcDataType::LOGICAL);
    subType1->set_logicalvalue(true);
    subType2->set_type(PlcDataType::INT);
    subType2->set_intvalue(2);

    EXPECT_EQ(ReturnStatus::OK, core->prepare(request));
    EXPECT_EQ(ReturnStatus::OK, core->execute());
    EXPECT_EQ(ReturnStatus::OK, core->getResults(response));
    EXPECT_NO_THROW(core->cleanup());
    EXPECT_EQ(1, response->results()[0].scalarvalue().intvalue());
    EXPECT_EQ(PlcDataType::INT, response->results()[0].type());
}

TEST_F(RConvTest, RConvArgUdtWithTwoElementsTestOneForName) {
    plcontainer::CallRequest *request = new plcontainer::CallRequest();
    plcontainer::CallResponse *response = new plcontainer::CallResponse();
    CompositeData *data;
    ScalarData *subType1;
    ScalarData *subType2;

    PlcValue *arg = request->add_args();

    ProcSrc *src = request->mutable_proc();
    ReturnType *ret = request->mutable_rettype();

    ret->set_type(PlcDataType::INT);

    src->set_name("test");
    src->set_src(
        "print(a)\n if ( (a['1','aa'] != TRUE) || (typeof(a['1','aa']) != 'logical') ) return "
        "(0)\n "
        "return(1)");
    arg->set_name("a");
    arg->set_type(PlcDataType::COMPOSITE);
    data = arg->mutable_compositevalue();
    subType1 = data->add_values();
    subType2 = data->add_values();

    subType1->set_type(PlcDataType::LOGICAL);
    subType1->set_logicalvalue(true);
    subType1->set_name("aa");
    subType2->set_type(PlcDataType::INT);
    subType2->set_name("bb");
    subType2->set_intvalue(2);

    EXPECT_EQ(ReturnStatus::OK, core->prepare(request));
    EXPECT_EQ(ReturnStatus::OK, core->execute());
    EXPECT_EQ(ReturnStatus::OK, core->getResults(response));
    EXPECT_NO_THROW(core->cleanup());
    EXPECT_EQ(1, response->results()[0].scalarvalue().intvalue());
    EXPECT_EQ(PlcDataType::INT, response->results()[0].type());
}

TEST_F(RConvTest, RConvArgUdtWithTwoElementsTestTwoForName) {
    plcontainer::CallRequest *request = new plcontainer::CallRequest();
    plcontainer::CallResponse *response = new plcontainer::CallResponse();
    CompositeData *data;
    ScalarData *subType1;
    ScalarData *subType2;

    PlcValue *arg = request->add_args();

    ProcSrc *src = request->mutable_proc();
    ReturnType *ret = request->mutable_rettype();

    ret->set_type(PlcDataType::INT);

    src->set_name("test");
    src->set_src(
        "print(a)\n if ( (a['1','bb'] != 2) || (typeof(a['1','bb']) != 'integer') ) return (0)\n "
        "return(1)");
    arg->set_name("a");
    arg->set_type(PlcDataType::COMPOSITE);
    data = arg->mutable_compositevalue();
    subType1 = data->add_values();
    subType2 = data->add_values();

    subType1->set_type(PlcDataType::LOGICAL);
    subType1->set_logicalvalue(true);
    subType1->set_name("aa");
    subType2->set_type(PlcDataType::INT);
    subType2->set_name("bb");
    subType2->set_intvalue(2);

    EXPECT_EQ(ReturnStatus::OK, core->prepare(request));
    EXPECT_EQ(ReturnStatus::OK, core->execute());
    EXPECT_EQ(ReturnStatus::OK, core->getResults(response));
    EXPECT_NO_THROW(core->cleanup());
    EXPECT_EQ(1, response->results()[0].scalarvalue().intvalue());
    EXPECT_EQ(PlcDataType::INT, response->results()[0].type());
}

TEST_F(RConvTest, RConvArgUdtWithTwoElementsTestReal) {
    plcontainer::CallRequest *request = new plcontainer::CallRequest();
    plcontainer::CallResponse *response = new plcontainer::CallResponse();
    CompositeData *data;
    ScalarData *subType1;
    ScalarData *subType2;

    PlcValue *arg = request->add_args();

    ProcSrc *src = request->mutable_proc();
    ReturnType *ret = request->mutable_rettype();

    ret->set_type(PlcDataType::INT);

    src->set_name("test");
    src->set_src(
        "print(a)\n if ( (a['1','bb'] != 2.1) || (typeof(a['1','bb']) != 'double') ) return (0)\n "
        "return(1)");
    arg->set_name("a");
    arg->set_type(PlcDataType::COMPOSITE);
    data = arg->mutable_compositevalue();
    subType1 = data->add_values();
    subType2 = data->add_values();

    subType1->set_type(PlcDataType::LOGICAL);
    subType1->set_logicalvalue(true);
    subType1->set_name("aa");
    subType2->set_type(PlcDataType::REAL);
    subType2->set_name("bb");
    subType2->set_realvalue(2.1);

    EXPECT_EQ(ReturnStatus::OK, core->prepare(request));
    EXPECT_EQ(ReturnStatus::OK, core->execute());
    EXPECT_EQ(ReturnStatus::OK, core->getResults(response));
    EXPECT_NO_THROW(core->cleanup());
    EXPECT_EQ(1, response->results()[0].scalarvalue().intvalue());
    EXPECT_EQ(PlcDataType::INT, response->results()[0].type());
}

TEST_F(RConvTest, RConvArgUdtWithTwoElementsTestText) {
    plcontainer::CallRequest *request = new plcontainer::CallRequest();
    plcontainer::CallResponse *response = new plcontainer::CallResponse();
    CompositeData *data;
    ScalarData *subType1;
    ScalarData *subType2;

    PlcValue *arg = request->add_args();

    ProcSrc *src = request->mutable_proc();
    ReturnType *ret = request->mutable_rettype();

    ret->set_type(PlcDataType::INT);

    src->set_name("test");
    src->set_src(
        "print(a)\n if ( (a['1','bb'] != '2') || (typeof(a['1','bb']) != 'character') ) return "
        "(0)\n "
        "return(1)");
    arg->set_name("a");
    arg->set_type(PlcDataType::COMPOSITE);
    data = arg->mutable_compositevalue();
    subType1 = data->add_values();
    subType2 = data->add_values();

    subType1->set_type(PlcDataType::LOGICAL);
    subType1->set_logicalvalue(true);
    subType1->set_name("aa");
    subType2->set_type(PlcDataType::TEXT);
    subType2->set_name("bb");
    subType2->set_stringvalue("2");

    EXPECT_EQ(ReturnStatus::OK, core->prepare(request));
    EXPECT_EQ(ReturnStatus::OK, core->execute());
    EXPECT_EQ(ReturnStatus::OK, core->getResults(response));
    EXPECT_NO_THROW(core->cleanup());
    EXPECT_EQ(1, response->results()[0].scalarvalue().intvalue());
    EXPECT_EQ(PlcDataType::INT, response->results()[0].type());
}

TEST_F(RConvTest, RConvResultsUdtWithTwoElements) {
    plcontainer::CallRequest *request = new plcontainer::CallRequest();
    plcontainer::CallResponse *response = new plcontainer::CallResponse();

    ProcSrc *src = request->mutable_proc();
    ReturnType *ret = request->mutable_rettype();

    src->set_name("test");
    src->set_src(
        "a=TRUE\n b=as.integer(1)\n c=as.integer(2)\n d=3\n e='foo'\n x<-data.frame(a,b,c,d,e)");
    ret->set_type(PlcDataType::COMPOSITE);
    ret->add_subtypes(PlcDataType::LOGICAL);
    ret->add_subtypes(PlcDataType::INT);

    EXPECT_EQ(ReturnStatus::OK, core->prepare(request));
    EXPECT_EQ(ReturnStatus::OK, core->execute());
    EXPECT_EQ(ReturnStatus::OK, core->getResults(response));
    EXPECT_EQ(true, response->results()[0].compositevalue().values()[0].logicalvalue());
    EXPECT_EQ(1, response->results()[0].compositevalue().values()[1].intvalue());
    EXPECT_EQ(PlcDataType::COMPOSITE, response->results()[0].type());
}

TEST_F(RConvTest, RConvResultsUdtWithFiveElements) {
    plcontainer::CallRequest *request = new plcontainer::CallRequest();
    plcontainer::CallResponse *response = new plcontainer::CallResponse();

    ProcSrc *src = request->mutable_proc();
    ReturnType *ret = request->mutable_rettype();

    src->set_name("test");
    src->set_src(
        "a=TRUE\n b=as.integer(1)\n c=as.integer(2)\n d=3\n e='foo'\n x<-data.frame(a,b,c,d,e)\n "
        "return(x)");
    ret->set_type(PlcDataType::COMPOSITE);
    ret->add_subtypes(PlcDataType::LOGICAL);
    ret->add_subtypes(PlcDataType::INT);
    ret->add_subtypes(PlcDataType::INT);
    ret->add_subtypes(PlcDataType::REAL);
    ret->add_subtypes(PlcDataType::TEXT);

    EXPECT_EQ(ReturnStatus::OK, core->prepare(request));
    EXPECT_EQ(ReturnStatus::OK, core->execute());
    EXPECT_EQ(ReturnStatus::OK, core->getResults(response));
    EXPECT_EQ(true, response->results()[0].compositevalue().values()[0].logicalvalue());
    EXPECT_EQ(1, response->results()[0].compositevalue().values()[1].intvalue());
    EXPECT_EQ(2, response->results()[0].compositevalue().values()[2].intvalue());
    EXPECT_EQ(3, response->results()[0].compositevalue().values()[3].realvalue());
    EXPECT_EQ("foo", response->results()[0].compositevalue().values()[4].stringvalue());
    EXPECT_EQ(PlcDataType::COMPOSITE, response->results()[0].type());
}

TEST_F(RConvTest, RConvResultsUdtWithFiveElementsError) {
    plcontainer::CallRequest *request = new plcontainer::CallRequest();
    plcontainer::CallResponse *response = new plcontainer::CallResponse();

    ProcSrc *src = request->mutable_proc();
    ReturnType *ret = request->mutable_rettype();

    src->set_name("test");
    src->set_src("return(10)");
    ret->set_type(PlcDataType::COMPOSITE);
    ret->add_subtypes(PlcDataType::LOGICAL);
    ret->add_subtypes(PlcDataType::INT);
    ret->add_subtypes(PlcDataType::INT);
    ret->add_subtypes(PlcDataType::REAL);
    ret->add_subtypes(PlcDataType::TEXT);

    EXPECT_EQ(ReturnStatus::OK, core->prepare(request));
    EXPECT_EQ(ReturnStatus::OK, core->execute());
    EXPECT_THROW(core->getResults(response), RServerWarningException);
    EXPECT_EQ(PlcDataType::COMPOSITE, response->results()[0].type());
}
