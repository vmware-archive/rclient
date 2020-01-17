#include <cmath>

#include "gtest/gtest.h"
#include "plcontainer.grpc.pb.h"
#include "plcontainer.pb.h"
#include "rcall.hh"
#include "rtypeconverter.hh"

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

TEST_F(RConvTest, RConvArgArrayWithOneArgsLogical) {
    plcontainer::CallRequest *request = new plcontainer::CallRequest();
    plcontainer::CallResponse *response = new plcontainer::CallResponse();
    ArrayData *data;
    ScalarData *element;

    PlcValue *arg = request->add_args();

    ProcSrc *src = request->mutable_proc();
    ReturnType *ret = request->mutable_rettype();

    ret->set_type(PlcDataType::INT);

    src->set_name("test");
    src->set_src("mean(a)");
    arg->set_name("a");
    arg->set_type(PlcDataType::ARRAY);
    data = arg->mutable_arrayvalue();
    data->set_elementtype(PlcDataType::LOGICAL);

    element = data->add_values();
    element->set_logicalvalue(true);
    element = data->add_values();
    element->set_logicalvalue(true);
    element = data->add_values();
    element->set_logicalvalue(true);

    EXPECT_EQ(ReturnStatus::OK, core->prepare(request));
    EXPECT_EQ(ReturnStatus::OK, core->execute());
    EXPECT_EQ(ReturnStatus::OK, core->getResults(response));
    EXPECT_NO_THROW(core->cleanup());
    EXPECT_EQ(1, response->results()[0].scalarvalue().intvalue());
    EXPECT_EQ(PlcDataType::INT, response->results()[0].type());
}

TEST_F(RConvTest, RConvArgArrayWithOneArgsINT) {
    plcontainer::CallRequest *request = new plcontainer::CallRequest();
    plcontainer::CallResponse *response = new plcontainer::CallResponse();
    ArrayData *data;
    ScalarData *element;

    PlcValue *arg = request->add_args();

    ProcSrc *src = request->mutable_proc();
    ReturnType *ret = request->mutable_rettype();

    ret->set_type(PlcDataType::INT);

    src->set_name("test");
    src->set_src("mean(a)");
    arg->set_name("a");
    arg->set_type(PlcDataType::ARRAY);
    data = arg->mutable_arrayvalue();
    data->set_elementtype(PlcDataType::INT);

    element = data->add_values();
    element->set_intvalue(1);
    element = data->add_values();
    element->set_intvalue(2);
    element = data->add_values();
    element->set_intvalue(3);

    EXPECT_EQ(ReturnStatus::OK, core->prepare(request));
    EXPECT_EQ(ReturnStatus::OK, core->execute());
    EXPECT_EQ(ReturnStatus::OK, core->getResults(response));
    EXPECT_NO_THROW(core->cleanup());
    EXPECT_EQ(2, response->results()[0].scalarvalue().intvalue());
    EXPECT_EQ(PlcDataType::INT, response->results()[0].type());
}

TEST_F(RConvTest, RConvArgArrayWithOneArgsReal) {
    plcontainer::CallRequest *request = new plcontainer::CallRequest();
    plcontainer::CallResponse *response = new plcontainer::CallResponse();
    ArrayData *data;
    ScalarData *element;

    PlcValue *arg = request->add_args();

    ProcSrc *src = request->mutable_proc();
    ReturnType *ret = request->mutable_rettype();

    ret->set_type(PlcDataType::REAL);

    src->set_name("test");
    src->set_src("mean(a)");
    arg->set_name("a");
    arg->set_type(PlcDataType::ARRAY);
    data = arg->mutable_arrayvalue();
    data->set_elementtype(PlcDataType::REAL);

    element = data->add_values();
    element->set_realvalue(0.1);
    element = data->add_values();
    element->set_realvalue(0.2);
    element = data->add_values();
    element->set_realvalue(0.3);

    EXPECT_EQ(ReturnStatus::OK, core->prepare(request));
    EXPECT_EQ(ReturnStatus::OK, core->execute());
    EXPECT_EQ(ReturnStatus::OK, core->getResults(response));
    EXPECT_NO_THROW(core->cleanup());
    EXPECT_EQ(0.2, response->results()[0].scalarvalue().realvalue());
    EXPECT_EQ(PlcDataType::REAL, response->results()[0].type());
}

TEST_F(RConvTest, RConvArgArrayWithOneArgsText) {
    plcontainer::CallRequest *request = new plcontainer::CallRequest();
    plcontainer::CallResponse *response = new plcontainer::CallResponse();
    ArrayData *data;
    ScalarData *element;

    PlcValue *arg = request->add_args();

    ProcSrc *src = request->mutable_proc();
    ReturnType *ret = request->mutable_rettype();

    ret->set_type(PlcDataType::TEXT);

    src->set_name("test");
    src->set_src("paste(a,collapse=' ')");
    arg->set_name("a");
    arg->set_type(PlcDataType::ARRAY);
    data = arg->mutable_arrayvalue();
    data->set_elementtype(PlcDataType::TEXT);

    element = data->add_values();
    element->set_stringvalue("a");
    element = data->add_values();
    element->set_stringvalue("b");
    element = data->add_values();
    element->set_stringvalue("c");

    EXPECT_EQ(ReturnStatus::OK, core->prepare(request));
    EXPECT_EQ(ReturnStatus::OK, core->execute());
    EXPECT_EQ(ReturnStatus::OK, core->getResults(response));
    EXPECT_NO_THROW(core->cleanup());
    EXPECT_EQ("a b c", response->results()[0].scalarvalue().stringvalue());
    EXPECT_EQ(PlcDataType::TEXT, response->results()[0].type());
}

TEST_F(RConvTest, RConvArgArrayWithOneArgsBytea) {
    plcontainer::CallRequest *request = new plcontainer::CallRequest();
    plcontainer::CallResponse *response = new plcontainer::CallResponse();
    ArrayData *data;
    ScalarData *element;

    PlcValue *arg = request->add_args();

    ProcSrc *src = request->mutable_proc();
    ReturnType *ret = request->mutable_rettype();

    ret->set_type(PlcDataType::TEXT);

    src->set_name("test");
    src->set_src("paste(a,collapse=' ')");
    arg->set_name("a");
    arg->set_type(PlcDataType::ARRAY);
    data = arg->mutable_arrayvalue();
    data->set_elementtype(PlcDataType::BYTEA);

    element = data->add_values();
    element->set_byteavalue("a");
    element = data->add_values();
    element->set_byteavalue("b");
    element = data->add_values();
    element->set_byteavalue("c");

    EXPECT_EQ(ReturnStatus::OK, core->prepare(request));
    EXPECT_EQ(ReturnStatus::OK, core->execute());
    EXPECT_EQ(ReturnStatus::OK, core->getResults(response));
    EXPECT_NO_THROW(core->cleanup());
    EXPECT_EQ("a b c", response->results()[0].scalarvalue().stringvalue());
    EXPECT_EQ(PlcDataType::TEXT, response->results()[0].type());
}

TEST_F(RConvTest, RConvArgArrayWithOneArgsNA) {
    plcontainer::CallRequest *request = new plcontainer::CallRequest();
    plcontainer::CallResponse *response = new plcontainer::CallResponse();
    ArrayData *data;
    ScalarData *element;

    PlcValue *arg = request->add_args();

    ProcSrc *src = request->mutable_proc();
    ReturnType *ret = request->mutable_rettype();

    ret->set_type(PlcDataType::REAL);

    src->set_name("test");
    src->set_src("mean(a)");
    arg->set_name("a");
    arg->set_type(PlcDataType::ARRAY);
    data = arg->mutable_arrayvalue();
    data->set_elementtype(PlcDataType::REAL);

    element = data->add_values();
    element->set_realvalue(0.1);
    element = data->add_values();
    element->set_isnull(true);
    element = data->add_values();
    element->set_realvalue(0.3);

    EXPECT_EQ(ReturnStatus::OK, core->prepare(request));
    EXPECT_EQ(ReturnStatus::OK, core->execute());
    EXPECT_EQ(ReturnStatus::OK, core->getResults(response));
    EXPECT_NO_THROW(core->cleanup());
    EXPECT_EQ(PlcDataType::REAL, response->results()[0].type());
}

TEST_F(RConvTest, RConvArgArrayWithTwoArgsINT) {
    plcontainer::CallRequest *request = new plcontainer::CallRequest();
    plcontainer::CallResponse *response = new plcontainer::CallResponse();
    ArrayData *data;
    ScalarData *element;

    PlcValue *arg1 = request->add_args();
    PlcValue *arg2 = request->add_args();

    ProcSrc *src = request->mutable_proc();
    ReturnType *ret = request->mutable_rettype();

    ret->set_type(PlcDataType::INT);

    src->set_name("test");
    src->set_src("mean(a*b)");
    arg1->set_name("a");
    arg1->set_type(PlcDataType::ARRAY);
    data = arg1->mutable_arrayvalue();
    data->set_elementtype(PlcDataType::INT);

    element = data->add_values();
    element->set_intvalue(1);
    element = data->add_values();
    element->set_intvalue(2);
    element = data->add_values();
    element->set_intvalue(3);

    arg2->set_name("b");
    arg2->set_type(PlcDataType::ARRAY);
    data = arg2->mutable_arrayvalue();
    data->set_elementtype(PlcDataType::INT);

    element = data->add_values();
    element->set_intvalue(1);
    element = data->add_values();
    element->set_intvalue(2);
    element = data->add_values();
    element->set_intvalue(3);

    EXPECT_EQ(ReturnStatus::OK, core->prepare(request));
    EXPECT_EQ(ReturnStatus::OK, core->execute());
    EXPECT_EQ(ReturnStatus::OK, core->getResults(response));
    EXPECT_NO_THROW(core->cleanup());
    EXPECT_EQ(4, response->results()[0].scalarvalue().intvalue());
    EXPECT_EQ(PlcDataType::INT, response->results()[0].type());
}

TEST_F(RConvTest, RConvReturnArrayWithTwoArgsINT) {
    plcontainer::CallRequest *request = new plcontainer::CallRequest();
    plcontainer::CallResponse *response = new plcontainer::CallResponse();
    ArrayData *data;
    ScalarData *element;

    PlcValue *arg1 = request->add_args();
    PlcValue *arg2 = request->add_args();

    ProcSrc *src = request->mutable_proc();
    ReturnType *ret = request->mutable_rettype();

    ret->set_type(PlcDataType::ARRAY);
    ret->add_subtypes(PlcDataType::INT);

    src->set_name("test");
    src->set_src("a*b");
    arg1->set_name("a");
    arg1->set_type(PlcDataType::ARRAY);
    data = arg1->mutable_arrayvalue();
    data->set_elementtype(PlcDataType::INT);

    element = data->add_values();
    element->set_intvalue(1);
    element = data->add_values();
    element->set_intvalue(2);
    element = data->add_values();
    element->set_intvalue(3);

    arg2->set_name("b");
    arg2->set_type(PlcDataType::ARRAY);
    data = arg2->mutable_arrayvalue();
    data->set_elementtype(PlcDataType::INT);

    element = data->add_values();
    element->set_intvalue(1);
    element = data->add_values();
    element->set_intvalue(2);
    element = data->add_values();
    element->set_intvalue(3);

    EXPECT_EQ(ReturnStatus::OK, core->prepare(request));
    EXPECT_EQ(ReturnStatus::OK, core->execute());
    EXPECT_EQ(ReturnStatus::OK, core->getResults(response));
    EXPECT_NO_THROW(core->cleanup());
    EXPECT_EQ(3, response->results()[0].arrayvalue().values_size());
    EXPECT_EQ(PlcDataType::ARRAY, response->results()[0].type());
}

TEST_F(RConvTest, RConvReturnArrayWithTwoArgsLOGICAL) {
    plcontainer::CallRequest *request = new plcontainer::CallRequest();
    plcontainer::CallResponse *response = new plcontainer::CallResponse();
    ArrayData *data;
    ScalarData *element;

    PlcValue *arg1 = request->add_args();
    PlcValue *arg2 = request->add_args();

    ProcSrc *src = request->mutable_proc();
    ReturnType *ret = request->mutable_rettype();

    ret->set_type(PlcDataType::ARRAY);
    ret->add_subtypes(PlcDataType::LOGICAL);

    src->set_name("test");
    src->set_src("a*b");
    arg1->set_name("a");
    arg1->set_type(PlcDataType::ARRAY);
    data = arg1->mutable_arrayvalue();
    data->set_elementtype(PlcDataType::LOGICAL);

    element = data->add_values();
    element->set_logicalvalue(true);
    element = data->add_values();
    element->set_logicalvalue(false);
    element = data->add_values();
    element->set_logicalvalue(true);

    arg2->set_name("b");
    arg2->set_type(PlcDataType::ARRAY);
    data = arg2->mutable_arrayvalue();
    data->set_elementtype(PlcDataType::LOGICAL);

    element = data->add_values();
    element->set_logicalvalue(true);
    element = data->add_values();
    element->set_logicalvalue(true);
    element = data->add_values();
    element->set_logicalvalue(false);

    EXPECT_EQ(ReturnStatus::OK, core->prepare(request));
    EXPECT_EQ(ReturnStatus::OK, core->execute());
    EXPECT_EQ(ReturnStatus::OK, core->getResults(response));
    EXPECT_NO_THROW(core->cleanup());
    EXPECT_EQ(3, response->results()[0].arrayvalue().values_size());
    EXPECT_EQ(PlcDataType::ARRAY, response->results()[0].type());
}

TEST_F(RConvTest, RConvReturnArrayWithTwoArgsREAL) {
    plcontainer::CallRequest *request = new plcontainer::CallRequest();
    plcontainer::CallResponse *response = new plcontainer::CallResponse();
    ArrayData *data;
    ScalarData *element;

    PlcValue *arg1 = request->add_args();
    PlcValue *arg2 = request->add_args();

    ProcSrc *src = request->mutable_proc();
    ReturnType *ret = request->mutable_rettype();

    ret->set_type(PlcDataType::ARRAY);
    ret->add_subtypes(PlcDataType::REAL);

    src->set_name("test");
    src->set_src("a*b");
    arg1->set_name("a");
    arg1->set_type(PlcDataType::ARRAY);
    data = arg1->mutable_arrayvalue();
    data->set_elementtype(PlcDataType::REAL);

    element = data->add_values();
    element->set_realvalue(1.5);
    element = data->add_values();
    element->set_realvalue(2.5);
    element = data->add_values();
    element->set_realvalue(3.5);

    arg2->set_name("b");
    arg2->set_type(PlcDataType::ARRAY);
    data = arg2->mutable_arrayvalue();
    data->set_elementtype(PlcDataType::REAL);

    element = data->add_values();
    element->set_realvalue(1.5);
    element = data->add_values();
    element->set_realvalue(2.5);
    element = data->add_values();
    element->set_realvalue(3.5);

    EXPECT_EQ(ReturnStatus::OK, core->prepare(request));
    EXPECT_EQ(ReturnStatus::OK, core->execute());
    EXPECT_EQ(ReturnStatus::OK, core->getResults(response));
    EXPECT_NO_THROW(core->cleanup());
    EXPECT_EQ(3, response->results()[0].arrayvalue().values_size());
    EXPECT_EQ(PlcDataType::ARRAY, response->results()[0].type());
}

TEST_F(RConvTest, RConvReturnArrayWithTwoArgsTEXT) {
    plcontainer::CallRequest *request = new plcontainer::CallRequest();
    plcontainer::CallResponse *response = new plcontainer::CallResponse();
    ArrayData *data;
    ScalarData *element;

    PlcValue *arg1 = request->add_args();
    PlcValue *arg2 = request->add_args();

    ProcSrc *src = request->mutable_proc();
    ReturnType *ret = request->mutable_rettype();

    ret->set_type(PlcDataType::ARRAY);
    ret->add_subtypes(PlcDataType::TEXT);

    src->set_name("test");
    src->set_src("a");
    arg1->set_name("a");
    arg1->set_type(PlcDataType::ARRAY);
    data = arg1->mutable_arrayvalue();
    data->set_elementtype(PlcDataType::TEXT);

    element = data->add_values();
    element->set_stringvalue("aa");
    element = data->add_values();
    element->set_stringvalue("bb");
    element = data->add_values();
    element->set_stringvalue("cc");

    arg2->set_name("b");
    arg2->set_type(PlcDataType::ARRAY);
    data = arg2->mutable_arrayvalue();
    data->set_elementtype(PlcDataType::TEXT);

    element = data->add_values();
    element->set_stringvalue("aa");
    element = data->add_values();
    element->set_stringvalue("bb");
    element = data->add_values();
    element->set_stringvalue("cc");

    EXPECT_EQ(ReturnStatus::OK, core->prepare(request));
    EXPECT_EQ(ReturnStatus::OK, core->execute());
    EXPECT_EQ(ReturnStatus::OK, core->getResults(response));
    EXPECT_NO_THROW(core->cleanup());
    EXPECT_EQ(3, response->results()[0].arrayvalue().values_size());
    EXPECT_EQ(PlcDataType::ARRAY, response->results()[0].type());
}

TEST_F(RConvTest, RConvArgArrayWithUDTError) {
    plcontainer::CallRequest *request = new plcontainer::CallRequest();
    ArrayData *data;
    ScalarData *element;

    PlcValue *arg = request->add_args();

    ProcSrc *src = request->mutable_proc();
    ReturnType *ret = request->mutable_rettype();

    ret->set_type(PlcDataType::TEXT);

    src->set_name("test");
    src->set_src("paste(a,collapse=' ')");
    arg->set_name("a");
    arg->set_type(PlcDataType::ARRAY);
    data = arg->mutable_arrayvalue();
    data->set_elementtype(PlcDataType::COMPOSITE);

    element = data->add_values();
    element->set_stringvalue("a");
    element = data->add_values();
    element->set_stringvalue("b");
    element = data->add_values();
    element->set_stringvalue("c");

    EXPECT_THROW(core->prepare(request), RServerWarningException);
}

TEST_F(RConvTest, RConvReturnArrayWithUDTError) {
    plcontainer::CallRequest *request = new plcontainer::CallRequest();
    plcontainer::CallResponse *response = new plcontainer::CallResponse();
    ArrayData *data;
    ScalarData *element;

    PlcValue *arg = request->add_args();

    ProcSrc *src = request->mutable_proc();
    ReturnType *ret = request->mutable_rettype();

    ret->set_type(PlcDataType::ARRAY);
    ret->add_subtypes(PlcDataType::COMPOSITE);

    src->set_name("test");
    src->set_src("mean(a)");
    arg->set_name("a");
    arg->set_type(PlcDataType::ARRAY);
    data = arg->mutable_arrayvalue();
    data->set_elementtype(PlcDataType::REAL);

    element = data->add_values();
    element->set_realvalue(0.1);
    element = data->add_values();
    element->set_isnull(true);
    element = data->add_values();
    element->set_realvalue(0.3);

    EXPECT_EQ(ReturnStatus::OK, core->prepare(request));
    EXPECT_EQ(ReturnStatus::OK, core->execute());
    EXPECT_THROW(core->getResults(response), RServerWarningException);
}

TEST_F(RConvTest, RConvReturnArrayWithOneArgsBYTEA) {
    plcontainer::CallRequest *request = new plcontainer::CallRequest();
    plcontainer::CallResponse *response = new plcontainer::CallResponse();
    // ArrayData *data;
    ScalarData *element;

    PlcValue *arg1 = request->add_args();

    ProcSrc *src = request->mutable_proc();
    ReturnType *ret = request->mutable_rettype();

    ret->set_type(PlcDataType::ARRAY);
    ret->add_subtypes(PlcDataType::INT);

    src->set_name("test");
    src->set_src("return (a)");
    arg1->set_name("a");
    arg1->set_type(PlcDataType::BYTEA);
    element = arg1->mutable_scalarvalue();
    element->set_type(PlcDataType::BYTEA);

    element->set_byteavalue(
        "X\n\000\000\000\003\000\003\006\000\000\003\005\000\000\000\000\005UTF-"
        "8\000\000\002\r\000\000\000\003\000\000\000{"
        "\000\000\000\001\000\000\000\007\000\000\004\002\000\000\000\001\000\004\000\t\000\000\000"
        "\003dim\000\000\000\r\000\000\000\001\000\000\000\003\000\000\000\376",
        79);

    EXPECT_EQ(ReturnStatus::OK, core->prepare(request));
    EXPECT_EQ(ReturnStatus::OK, core->execute());
    EXPECT_EQ(ReturnStatus::OK, core->getResults(response));
    EXPECT_NO_THROW(core->cleanup());
    EXPECT_EQ(3, response->results()[0].arrayvalue().values_size());
    EXPECT_EQ(PlcDataType::ARRAY, response->results()[0].type());
}