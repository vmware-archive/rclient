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
        core->initRProtectList();
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
    EXPECT_THROW(core->getResults(response), RServerErrorException);
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

    EXPECT_THROW(core->prepare(request), RServerErrorException);
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
    EXPECT_THROW(core->getResults(response), RServerErrorException);
}

TEST_F(RConvTest, RConvReturnArrayWithOneArgsBYTEA) {
    plcontainer::CallRequest *request = new plcontainer::CallRequest();
    plcontainer::CallResponse *response = new plcontainer::CallResponse();
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

TEST_F(RConvTest, RConvArgSetofOne) {
    plcontainer::CallRequest *request = new plcontainer::CallRequest();
    plcontainer::CallResponse *response = new plcontainer::CallResponse();
    SetOfData *data;
    ScalarData *element;
    CompositeData *row;

    PlcValue *arg1 = request->add_args();

    ProcSrc *src = request->mutable_proc();
    ReturnType *ret = request->mutable_rettype();

    ret->set_type(PlcDataType::INT);

    src->set_name("test");
    src->set_src("return (nrow(a))");
    arg1->set_name("a");
    arg1->set_type(PlcDataType::SETOF);

    data = arg1->mutable_setofvalue();
    data->add_columnnames();
    data->set_columnnames(0, "0");
    data->add_columnnames();
    data->set_columnnames(1, "1");
    data->add_columntypes(PlcDataType::INT);
    data->add_columntypes(PlcDataType::INT);

    // row 1
    row = data->add_rowvalues();
    element = row->add_values();
    element->set_intvalue(1);
    element = row->add_values();
    element->set_intvalue(2);

    // row 2
    row = data->add_rowvalues();
    element = row->add_values();
    element->set_intvalue(1);
    element = row->add_values();
    element->set_intvalue(2);

    EXPECT_EQ(ReturnStatus::OK, core->prepare(request));
    EXPECT_EQ(ReturnStatus::OK, core->execute());
    EXPECT_EQ(ReturnStatus::OK, core->getResults(response));
    EXPECT_NO_THROW(core->cleanup());
    EXPECT_EQ(2, response->results()[0].scalarvalue().intvalue());
    EXPECT_EQ(PlcDataType::INT, response->results()[0].type());
}

TEST_F(RConvTest, RConvArgSetofTwo) {
    plcontainer::CallRequest *request = new plcontainer::CallRequest();
    plcontainer::CallResponse *response = new plcontainer::CallResponse();
    SetOfData *data;
    ScalarData *element;
    CompositeData *row;
    PlcValue *arg;

    ProcSrc *src = request->mutable_proc();
    ReturnType *ret = request->mutable_rettype();

    ret->set_type(PlcDataType::INT);

    src->set_name("test");
    src->set_src("return (nrow(a) + nrow(b))");

    // Set firsr arg
    arg = request->add_args();
    arg->set_name("a");
    arg->set_type(PlcDataType::SETOF);

    data = arg->mutable_setofvalue();
    data->add_columnnames();
    data->set_columnnames(0, "0");
    data->add_columnnames();
    data->set_columnnames(1, "1");
    data->add_columntypes(PlcDataType::INT);
    data->add_columntypes(PlcDataType::INT);

    // row 1
    row = data->add_rowvalues();
    element = row->add_values();
    element->set_intvalue(1);
    element = row->add_values();
    element->set_intvalue(2);

    // row 2
    row = data->add_rowvalues();
    element = row->add_values();
    element->set_intvalue(1);
    element = row->add_values();
    element->set_intvalue(2);

    // Set second arg
    arg = request->add_args();
    arg->set_name("b");
    arg->set_type(PlcDataType::SETOF);

    data = arg->mutable_setofvalue();
    data->add_columnnames();
    data->set_columnnames(0, "0");
    data->add_columnnames();
    data->set_columnnames(1, "1");
    data->add_columntypes(PlcDataType::INT);
    data->add_columntypes(PlcDataType::INT);

    // row 1
    row = data->add_rowvalues();
    element = row->add_values();
    element->set_intvalue(1);
    element = row->add_values();
    element->set_intvalue(2);

    // row 2
    row = data->add_rowvalues();
    element = row->add_values();
    element->set_intvalue(1);
    element = row->add_values();
    element->set_intvalue(2);

    EXPECT_EQ(ReturnStatus::OK, core->prepare(request));
    EXPECT_EQ(ReturnStatus::OK, core->execute());
    EXPECT_EQ(ReturnStatus::OK, core->getResults(response));
    EXPECT_NO_THROW(core->cleanup());
    EXPECT_EQ(4, response->results()[0].scalarvalue().intvalue());
    EXPECT_EQ(PlcDataType::INT, response->results()[0].type());
}

TEST_F(RConvTest, RConvReturnSetofOneArg) {
    plcontainer::CallRequest *request = new plcontainer::CallRequest();
    plcontainer::CallResponse *response = new plcontainer::CallResponse();
    SetOfData *data;
    ScalarData *element;
    CompositeData *row;

    PlcValue *arg1 = request->add_args();

    ProcSrc *src = request->mutable_proc();
    ReturnType *ret = request->mutable_rettype();

    ret->set_type(PlcDataType::SETOF);
    ret->add_subtypes(PlcDataType::INT);
    ret->add_subtypes(PlcDataType::INT);

    src->set_name("test");
    src->set_src("return (data.frame('0'=1:2, '1'=1:2))");
    arg1->set_name("a");
    arg1->set_type(PlcDataType::SETOF);

    data = arg1->mutable_setofvalue();
    data->add_columnnames();
    data->set_columnnames(0, "0");
    data->add_columnnames();
    data->set_columnnames(1, "1");
    data->add_columntypes(PlcDataType::INT);
    data->add_columntypes(PlcDataType::INT);

    // row 1
    row = data->add_rowvalues();
    element = row->add_values();
    element->set_intvalue(1);
    element = row->add_values();
    element->set_intvalue(2);

    // row 2
    row = data->add_rowvalues();
    element = row->add_values();
    element->set_intvalue(1);
    element = row->add_values();
    element->set_intvalue(2);

    EXPECT_EQ(ReturnStatus::OK, core->prepare(request));
    EXPECT_EQ(ReturnStatus::OK, core->execute());
    EXPECT_EQ(ReturnStatus::OK, core->getResults(response));
    EXPECT_NO_THROW(core->cleanup());
    EXPECT_EQ(1, response->results()[0].setofvalue().rowvalues(0).values(0).intvalue());
    EXPECT_EQ(1, response->results()[0].setofvalue().rowvalues(0).values(1).intvalue());
    EXPECT_EQ(2, response->results()[0].setofvalue().rowvalues(1).values(0).intvalue());
    EXPECT_EQ(2, response->results()[0].setofvalue().rowvalues(1).values(1).intvalue());
    EXPECT_EQ(PlcDataType::SETOF, response->results()[0].type());
}

TEST_F(RConvTest, RConvReturnSetofFromOneArgINT) {
    plcontainer::CallRequest *request = new plcontainer::CallRequest();
    plcontainer::CallResponse *response = new plcontainer::CallResponse();
    SetOfData *data;
    ScalarData *element;
    CompositeData *row;

    PlcValue *arg1 = request->add_args();

    ProcSrc *src = request->mutable_proc();
    ReturnType *ret = request->mutable_rettype();

    ret->set_type(PlcDataType::SETOF);
    ret->add_subtypes(PlcDataType::INT);
    ret->add_subtypes(PlcDataType::INT);

    src->set_name("test");
    src->set_src("return (a)");
    arg1->set_name("a");
    arg1->set_type(PlcDataType::SETOF);

    data = arg1->mutable_setofvalue();
    data->add_columnnames();
    data->set_columnnames(0, "0");
    data->add_columnnames();
    data->set_columnnames(1, "1");
    data->add_columntypes(PlcDataType::INT);
    data->add_columntypes(PlcDataType::INT);

    // row 1
    row = data->add_rowvalues();
    element = row->add_values();
    element->set_intvalue(1);
    element = row->add_values();
    element->set_intvalue(2);

    // row 2
    row = data->add_rowvalues();
    element = row->add_values();
    element->set_intvalue(1);
    element = row->add_values();
    element->set_intvalue(2);

    EXPECT_EQ(ReturnStatus::OK, core->prepare(request));
    EXPECT_EQ(ReturnStatus::OK, core->execute());
    EXPECT_EQ(ReturnStatus::OK, core->getResults(response));
    EXPECT_NO_THROW(core->cleanup());
    EXPECT_EQ(1, response->results()[0].setofvalue().rowvalues(0).values(0).intvalue());
    EXPECT_EQ(2, response->results()[0].setofvalue().rowvalues(0).values(1).intvalue());
    EXPECT_EQ(1, response->results()[0].setofvalue().rowvalues(1).values(0).intvalue());
    EXPECT_EQ(2, response->results()[0].setofvalue().rowvalues(1).values(1).intvalue());
    EXPECT_EQ(PlcDataType::SETOF, response->results()[0].type());
}

TEST_F(RConvTest, RConvReturnSetofFromOneArgLOGICAL) {
    plcontainer::CallRequest *request = new plcontainer::CallRequest();
    plcontainer::CallResponse *response = new plcontainer::CallResponse();
    SetOfData *data;
    ScalarData *element;
    CompositeData *row;

    PlcValue *arg1 = request->add_args();

    ProcSrc *src = request->mutable_proc();
    ReturnType *ret = request->mutable_rettype();

    ret->set_type(PlcDataType::SETOF);
    ret->add_subtypes(PlcDataType::LOGICAL);
    ret->add_subtypes(PlcDataType::LOGICAL);

    src->set_name("test");
    src->set_src("return (a)");
    arg1->set_name("a");
    arg1->set_type(PlcDataType::SETOF);

    data = arg1->mutable_setofvalue();
    data->add_columnnames();
    data->set_columnnames(0, "0");
    data->add_columnnames();
    data->set_columnnames(1, "1");
    data->add_columntypes(PlcDataType::LOGICAL);
    data->add_columntypes(PlcDataType::LOGICAL);

    // row 1
    row = data->add_rowvalues();
    element = row->add_values();
    element->set_logicalvalue(true);
    element = row->add_values();
    element->set_logicalvalue(true);

    // row 2
    row = data->add_rowvalues();
    element = row->add_values();
    element->set_logicalvalue(false);
    element = row->add_values();
    element->set_logicalvalue(false);

    EXPECT_EQ(ReturnStatus::OK, core->prepare(request));
    EXPECT_EQ(ReturnStatus::OK, core->execute());
    EXPECT_EQ(ReturnStatus::OK, core->getResults(response));
    EXPECT_NO_THROW(core->cleanup());
    EXPECT_EQ(true, response->results()[0].setofvalue().rowvalues(0).values(0).logicalvalue());
    EXPECT_EQ(true, response->results()[0].setofvalue().rowvalues(0).values(1).logicalvalue());
    EXPECT_EQ(false, response->results()[0].setofvalue().rowvalues(1).values(0).logicalvalue());
    EXPECT_EQ(false, response->results()[0].setofvalue().rowvalues(1).values(1).logicalvalue());
    EXPECT_EQ(PlcDataType::SETOF, response->results()[0].type());
}

TEST_F(RConvTest, RConvReturnSetofFromIntArgToLOGICAL) {
    plcontainer::CallRequest *request = new plcontainer::CallRequest();
    plcontainer::CallResponse *response = new plcontainer::CallResponse();
    SetOfData *data;
    ScalarData *element;
    CompositeData *row;

    PlcValue *arg1 = request->add_args();

    ProcSrc *src = request->mutable_proc();
    ReturnType *ret = request->mutable_rettype();

    ret->set_type(PlcDataType::SETOF);
    ret->add_subtypes(PlcDataType::LOGICAL);
    ret->add_subtypes(PlcDataType::LOGICAL);

    src->set_name("test");
    src->set_src("return (a)");
    arg1->set_name("a");
    arg1->set_type(PlcDataType::SETOF);

    data = arg1->mutable_setofvalue();
    data->add_columnnames();
    data->set_columnnames(0, "0");
    data->add_columnnames();
    data->set_columnnames(1, "1");
    data->add_columntypes(PlcDataType::INT);
    data->add_columntypes(PlcDataType::INT);

    // row 1
    row = data->add_rowvalues();
    element = row->add_values();
    element->set_intvalue(1);
    element = row->add_values();
    element->set_intvalue(2);

    // row 2
    row = data->add_rowvalues();
    element = row->add_values();
    element->set_intvalue(0);
    element = row->add_values();
    element->set_intvalue(0);

    EXPECT_EQ(ReturnStatus::OK, core->prepare(request));
    EXPECT_EQ(ReturnStatus::OK, core->execute());
    EXPECT_EQ(ReturnStatus::OK, core->getResults(response));
    EXPECT_NO_THROW(core->cleanup());
    EXPECT_EQ(true, response->results()[0].setofvalue().rowvalues(0).values(0).logicalvalue());
    EXPECT_EQ(true, response->results()[0].setofvalue().rowvalues(0).values(1).logicalvalue());
    EXPECT_EQ(false, response->results()[0].setofvalue().rowvalues(1).values(0).logicalvalue());
    EXPECT_EQ(false, response->results()[0].setofvalue().rowvalues(1).values(1).logicalvalue());
    EXPECT_EQ(PlcDataType::SETOF, response->results()[0].type());
}

TEST_F(RConvTest, RConvReturnSetofFromIntArgToREAL) {
    plcontainer::CallRequest *request = new plcontainer::CallRequest();
    plcontainer::CallResponse *response = new plcontainer::CallResponse();
    SetOfData *data;
    ScalarData *element;
    CompositeData *row;

    PlcValue *arg1 = request->add_args();

    ProcSrc *src = request->mutable_proc();
    ReturnType *ret = request->mutable_rettype();

    ret->set_type(PlcDataType::SETOF);
    ret->add_subtypes(PlcDataType::REAL);
    ret->add_subtypes(PlcDataType::REAL);

    src->set_name("test");
    src->set_src("return (a)");
    arg1->set_name("a");
    arg1->set_type(PlcDataType::SETOF);

    data = arg1->mutable_setofvalue();
    data->add_columnnames();
    data->set_columnnames(0, "0");
    data->add_columnnames();
    data->set_columnnames(1, "1");
    data->add_columntypes(PlcDataType::INT);
    data->add_columntypes(PlcDataType::INT);

    // row 1
    row = data->add_rowvalues();
    element = row->add_values();
    element->set_intvalue(1);
    element = row->add_values();
    element->set_intvalue(2);

    // row 2
    row = data->add_rowvalues();
    element = row->add_values();
    element->set_intvalue(1);
    element = row->add_values();
    element->set_intvalue(2);

    EXPECT_EQ(ReturnStatus::OK, core->prepare(request));
    EXPECT_EQ(ReturnStatus::OK, core->execute());
    EXPECT_EQ(ReturnStatus::OK, core->getResults(response));
    EXPECT_NO_THROW(core->cleanup());
    EXPECT_EQ(1, response->results()[0].setofvalue().rowvalues(0).values(0).realvalue());
    EXPECT_EQ(2, response->results()[0].setofvalue().rowvalues(0).values(1).realvalue());
    EXPECT_EQ(1, response->results()[0].setofvalue().rowvalues(1).values(0).realvalue());
    EXPECT_EQ(2, response->results()[0].setofvalue().rowvalues(1).values(1).realvalue());
    EXPECT_EQ(PlcDataType::SETOF, response->results()[0].type());
}

TEST_F(RConvTest, RConvReturnSetofFromIntArgToINT) {
    plcontainer::CallRequest *request = new plcontainer::CallRequest();
    plcontainer::CallResponse *response = new plcontainer::CallResponse();
    SetOfData *data;
    ScalarData *element;
    CompositeData *row;

    PlcValue *arg1 = request->add_args();

    ProcSrc *src = request->mutable_proc();
    ReturnType *ret = request->mutable_rettype();

    ret->set_type(PlcDataType::SETOF);
    ret->add_subtypes(PlcDataType::INT);
    ret->add_subtypes(PlcDataType::INT);

    src->set_name("test");
    src->set_src("return (a)");
    arg1->set_name("a");
    arg1->set_type(PlcDataType::SETOF);

    data = arg1->mutable_setofvalue();
    data->add_columnnames();
    data->set_columnnames(0, "0");
    data->add_columnnames();
    data->set_columnnames(1, "1");
    data->add_columntypes(PlcDataType::INT);
    data->add_columntypes(PlcDataType::INT);

    // row 1
    row = data->add_rowvalues();
    element = row->add_values();
    element->set_intvalue(1);
    element = row->add_values();
    element->set_intvalue(2);

    // row 2
    row = data->add_rowvalues();
    element = row->add_values();
    element->set_intvalue(1);
    element = row->add_values();
    element->set_intvalue(2);

    EXPECT_EQ(ReturnStatus::OK, core->prepare(request));
    EXPECT_EQ(ReturnStatus::OK, core->execute());
    EXPECT_EQ(ReturnStatus::OK, core->getResults(response));
    EXPECT_NO_THROW(core->cleanup());
    EXPECT_EQ(1, response->results()[0].setofvalue().rowvalues(0).values(0).intvalue());
    EXPECT_EQ(2, response->results()[0].setofvalue().rowvalues(0).values(1).intvalue());
    EXPECT_EQ(1, response->results()[0].setofvalue().rowvalues(1).values(0).intvalue());
    EXPECT_EQ(2, response->results()[0].setofvalue().rowvalues(1).values(1).intvalue());
    EXPECT_EQ(PlcDataType::SETOF, response->results()[0].type());
}

TEST_F(RConvTest, RConvReturnSetofFromIntArgToTEXT) {
    plcontainer::CallRequest *request = new plcontainer::CallRequest();
    plcontainer::CallResponse *response = new plcontainer::CallResponse();
    SetOfData *data;
    ScalarData *element;
    CompositeData *row;

    PlcValue *arg1 = request->add_args();

    ProcSrc *src = request->mutable_proc();
    ReturnType *ret = request->mutable_rettype();

    ret->set_type(PlcDataType::SETOF);
    ret->add_subtypes(PlcDataType::TEXT);
    ret->add_subtypes(PlcDataType::TEXT);

    src->set_name("test");
    src->set_src("return (a)");
    arg1->set_name("a");
    arg1->set_type(PlcDataType::SETOF);

    data = arg1->mutable_setofvalue();
    data->add_columnnames();
    data->set_columnnames(0, "0");
    data->add_columnnames();
    data->set_columnnames(1, "1");
    data->add_columntypes(PlcDataType::INT);
    data->add_columntypes(PlcDataType::INT);

    // row 1
    row = data->add_rowvalues();
    element = row->add_values();
    element->set_intvalue(1);
    element = row->add_values();
    element->set_intvalue(2);

    // row 2
    row = data->add_rowvalues();
    element = row->add_values();
    element->set_intvalue(1);
    element = row->add_values();
    element->set_intvalue(2);

    EXPECT_EQ(ReturnStatus::OK, core->prepare(request));
    EXPECT_EQ(ReturnStatus::OK, core->execute());
    EXPECT_EQ(ReturnStatus::OK, core->getResults(response));
    EXPECT_NO_THROW(core->cleanup());
    EXPECT_EQ("1", response->results()[0].setofvalue().rowvalues(0).values(0).stringvalue());
    EXPECT_EQ("2", response->results()[0].setofvalue().rowvalues(0).values(1).stringvalue());
    EXPECT_EQ("1", response->results()[0].setofvalue().rowvalues(1).values(0).stringvalue());
    EXPECT_EQ("2", response->results()[0].setofvalue().rowvalues(1).values(1).stringvalue());
    EXPECT_EQ(PlcDataType::SETOF, response->results()[0].type());
}

TEST_F(RConvTest, RConvReturnSetofFromIntArgToRAW) {
    plcontainer::CallRequest *request = new plcontainer::CallRequest();
    plcontainer::CallResponse *response = new plcontainer::CallResponse();
    SetOfData *data;
    ScalarData *element;
    CompositeData *row;

    PlcValue *arg1 = request->add_args();

    ProcSrc *src = request->mutable_proc();
    ReturnType *ret = request->mutable_rettype();

    ret->set_type(PlcDataType::SETOF);
    ret->add_subtypes(PlcDataType::BYTEA);
    ret->add_subtypes(PlcDataType::BYTEA);

    src->set_name("test");
    src->set_src("return (a)");
    arg1->set_name("a");
    arg1->set_type(PlcDataType::SETOF);

    data = arg1->mutable_setofvalue();
    data->add_columnnames();
    data->set_columnnames(0, "0");
    data->add_columnnames();
    data->set_columnnames(1, "1");
    data->add_columntypes(PlcDataType::INT);
    data->add_columntypes(PlcDataType::INT);

    // row 1
    row = data->add_rowvalues();
    element = row->add_values();
    element->set_intvalue(1);
    element = row->add_values();
    element->set_intvalue(2);

    // row 2
    row = data->add_rowvalues();
    element = row->add_values();
    element->set_intvalue(1);
    element = row->add_values();
    element->set_intvalue(2);

    EXPECT_EQ(ReturnStatus::OK, core->prepare(request));
    EXPECT_EQ(ReturnStatus::OK, core->execute());
    EXPECT_EQ(ReturnStatus::OK, core->getResults(response));
    EXPECT_NO_THROW(core->cleanup());
    EXPECT_EQ("1", response->results()[0].setofvalue().rowvalues(0).values(0).byteavalue());
    EXPECT_EQ("2", response->results()[0].setofvalue().rowvalues(0).values(1).byteavalue());
    EXPECT_EQ("1", response->results()[0].setofvalue().rowvalues(1).values(0).byteavalue());
    EXPECT_EQ("2", response->results()[0].setofvalue().rowvalues(1).values(1).byteavalue());
    EXPECT_EQ(PlcDataType::SETOF, response->results()[0].type());
}

TEST_F(RConvTest, RConvReturnSetofFromMartixInt) {
    plcontainer::CallRequest *request = new plcontainer::CallRequest();
    plcontainer::CallResponse *response = new plcontainer::CallResponse();
    SetOfData *data;
    ScalarData *element;
    CompositeData *row;

    PlcValue *arg1 = request->add_args();

    ProcSrc *src = request->mutable_proc();
    ReturnType *ret = request->mutable_rettype();

    ret->set_type(PlcDataType::SETOF);
    ret->add_subtypes(PlcDataType::INT);
    ret->add_subtypes(PlcDataType::INT);

    src->set_name("test");
    src->set_src("return (matrix(0:3, nrow = 2, ncol = 2))");
    arg1->set_name("a");
    arg1->set_type(PlcDataType::SETOF);

    data = arg1->mutable_setofvalue();
    data->add_columnnames();
    data->set_columnnames(0, "0");
    data->add_columnnames();
    data->set_columnnames(1, "1");
    data->add_columntypes(PlcDataType::INT);
    data->add_columntypes(PlcDataType::INT);

    // row 1
    row = data->add_rowvalues();
    element = row->add_values();
    element->set_intvalue(1);
    element = row->add_values();
    element->set_intvalue(2);

    // row 2
    row = data->add_rowvalues();
    element = row->add_values();
    element->set_intvalue(1);
    element = row->add_values();
    element->set_intvalue(2);

    EXPECT_EQ(ReturnStatus::OK, core->prepare(request));
    EXPECT_EQ(ReturnStatus::OK, core->execute());
    EXPECT_EQ(ReturnStatus::OK, core->getResults(response));
    EXPECT_NO_THROW(core->cleanup());
    EXPECT_EQ(0, response->results()[0].setofvalue().rowvalues(0).values(0).intvalue());
    EXPECT_EQ(2, response->results()[0].setofvalue().rowvalues(0).values(1).intvalue());
    EXPECT_EQ(1, response->results()[0].setofvalue().rowvalues(1).values(0).intvalue());
    EXPECT_EQ(3, response->results()[0].setofvalue().rowvalues(1).values(1).intvalue());
    EXPECT_EQ(PlcDataType::SETOF, response->results()[0].type());
}

TEST_F(RConvTest, RConvReturnSetofFromMartixLogical) {
    plcontainer::CallRequest *request = new plcontainer::CallRequest();
    plcontainer::CallResponse *response = new plcontainer::CallResponse();
    SetOfData *data;
    ScalarData *element;
    CompositeData *row;

    PlcValue *arg1 = request->add_args();

    ProcSrc *src = request->mutable_proc();
    ReturnType *ret = request->mutable_rettype();

    ret->set_type(PlcDataType::SETOF);
    ret->add_subtypes(PlcDataType::LOGICAL);
    ret->add_subtypes(PlcDataType::LOGICAL);

    src->set_name("test");
    src->set_src("return (matrix(0:3, nrow = 2, ncol = 2))");
    arg1->set_name("a");
    arg1->set_type(PlcDataType::SETOF);

    data = arg1->mutable_setofvalue();
    data->add_columnnames();
    data->set_columnnames(0, "0");
    data->add_columnnames();
    data->set_columnnames(1, "1");
    data->add_columntypes(PlcDataType::INT);
    data->add_columntypes(PlcDataType::INT);

    // row 1
    row = data->add_rowvalues();
    element = row->add_values();
    element->set_intvalue(1);
    element = row->add_values();
    element->set_intvalue(2);

    // row 2
    row = data->add_rowvalues();
    element = row->add_values();
    element->set_intvalue(1);
    element = row->add_values();
    element->set_intvalue(2);

    EXPECT_EQ(ReturnStatus::OK, core->prepare(request));
    EXPECT_EQ(ReturnStatus::OK, core->execute());
    EXPECT_EQ(ReturnStatus::OK, core->getResults(response));
    EXPECT_NO_THROW(core->cleanup());
    EXPECT_EQ(false, response->results()[0].setofvalue().rowvalues(0).values(0).logicalvalue());
    EXPECT_EQ(true, response->results()[0].setofvalue().rowvalues(0).values(1).logicalvalue());
    EXPECT_EQ(true, response->results()[0].setofvalue().rowvalues(1).values(0).logicalvalue());
    EXPECT_EQ(true, response->results()[0].setofvalue().rowvalues(1).values(1).logicalvalue());
    EXPECT_EQ(PlcDataType::SETOF, response->results()[0].type());
}

TEST_F(RConvTest, RConvReturnSetofFromMartixReal) {
    plcontainer::CallRequest *request = new plcontainer::CallRequest();
    plcontainer::CallResponse *response = new plcontainer::CallResponse();
    SetOfData *data;
    ScalarData *element;
    CompositeData *row;

    PlcValue *arg1 = request->add_args();

    ProcSrc *src = request->mutable_proc();
    ReturnType *ret = request->mutable_rettype();

    ret->set_type(PlcDataType::SETOF);
    ret->add_subtypes(PlcDataType::REAL);
    ret->add_subtypes(PlcDataType::REAL);

    src->set_name("test");
    src->set_src("return (matrix(0:3, nrow = 2, ncol = 2))");
    arg1->set_name("a");
    arg1->set_type(PlcDataType::SETOF);

    data = arg1->mutable_setofvalue();
    data->add_columnnames();
    data->set_columnnames(0, "0");
    data->add_columnnames();
    data->set_columnnames(1, "1");
    data->add_columntypes(PlcDataType::INT);
    data->add_columntypes(PlcDataType::INT);

    // row 1
    row = data->add_rowvalues();
    element = row->add_values();
    element->set_intvalue(1);
    element = row->add_values();
    element->set_intvalue(2);

    // row 2
    row = data->add_rowvalues();
    element = row->add_values();
    element->set_intvalue(1);
    element = row->add_values();
    element->set_intvalue(2);

    EXPECT_EQ(ReturnStatus::OK, core->prepare(request));
    EXPECT_EQ(ReturnStatus::OK, core->execute());
    EXPECT_EQ(ReturnStatus::OK, core->getResults(response));
    EXPECT_NO_THROW(core->cleanup());
    EXPECT_EQ(0, response->results()[0].setofvalue().rowvalues(0).values(0).realvalue());
    EXPECT_EQ(2, response->results()[0].setofvalue().rowvalues(0).values(1).realvalue());
    EXPECT_EQ(1, response->results()[0].setofvalue().rowvalues(1).values(0).realvalue());
    EXPECT_EQ(3, response->results()[0].setofvalue().rowvalues(1).values(1).realvalue());
    EXPECT_EQ(PlcDataType::SETOF, response->results()[0].type());
}

TEST_F(RConvTest, RConvReturnSetofFromMartixText) {
    plcontainer::CallRequest *request = new plcontainer::CallRequest();
    plcontainer::CallResponse *response = new plcontainer::CallResponse();
    SetOfData *data;
    ScalarData *element;
    CompositeData *row;

    PlcValue *arg1 = request->add_args();

    ProcSrc *src = request->mutable_proc();
    ReturnType *ret = request->mutable_rettype();

    ret->set_type(PlcDataType::SETOF);
    ret->add_subtypes(PlcDataType::TEXT);
    ret->add_subtypes(PlcDataType::TEXT);

    src->set_name("test");
    src->set_src("return (matrix(0:3, nrow = 2, ncol = 2))");
    arg1->set_name("a");
    arg1->set_type(PlcDataType::SETOF);

    data = arg1->mutable_setofvalue();
    data->add_columnnames();
    data->set_columnnames(0, "0");
    data->add_columnnames();
    data->set_columnnames(1, "1");
    data->add_columntypes(PlcDataType::INT);
    data->add_columntypes(PlcDataType::INT);

    // row 1
    row = data->add_rowvalues();
    element = row->add_values();
    element->set_intvalue(1);
    element = row->add_values();
    element->set_intvalue(2);

    // row 2
    row = data->add_rowvalues();
    element = row->add_values();
    element->set_intvalue(1);
    element = row->add_values();
    element->set_intvalue(2);

    EXPECT_EQ(ReturnStatus::OK, core->prepare(request));
    EXPECT_EQ(ReturnStatus::OK, core->execute());
    EXPECT_EQ(ReturnStatus::OK, core->getResults(response));
    EXPECT_NO_THROW(core->cleanup());
    EXPECT_EQ("0", response->results()[0].setofvalue().rowvalues(0).values(0).stringvalue());
    EXPECT_EQ("2", response->results()[0].setofvalue().rowvalues(0).values(1).stringvalue());
    EXPECT_EQ("1", response->results()[0].setofvalue().rowvalues(1).values(0).stringvalue());
    EXPECT_EQ("3", response->results()[0].setofvalue().rowvalues(1).values(1).stringvalue());
    EXPECT_EQ(PlcDataType::SETOF, response->results()[0].type());
}

TEST_F(RConvTest, RConvReturnSetofFromMartixRAW) {
    plcontainer::CallRequest *request = new plcontainer::CallRequest();
    plcontainer::CallResponse *response = new plcontainer::CallResponse();
    SetOfData *data;
    ScalarData *element;
    CompositeData *row;

    PlcValue *arg1 = request->add_args();

    ProcSrc *src = request->mutable_proc();
    ReturnType *ret = request->mutable_rettype();

    ret->set_type(PlcDataType::SETOF);
    ret->add_subtypes(PlcDataType::BYTEA);
    ret->add_subtypes(PlcDataType::BYTEA);

    src->set_name("test");
    src->set_src("return (matrix(0:3, nrow = 2, ncol = 2))");
    arg1->set_name("a");
    arg1->set_type(PlcDataType::SETOF);

    data = arg1->mutable_setofvalue();
    data->add_columnnames();
    data->set_columnnames(0, "0");
    data->add_columnnames();
    data->set_columnnames(1, "1");
    data->add_columntypes(PlcDataType::INT);
    data->add_columntypes(PlcDataType::INT);

    // row 1
    row = data->add_rowvalues();
    element = row->add_values();
    element->set_intvalue(1);
    element = row->add_values();
    element->set_intvalue(2);

    // row 2
    row = data->add_rowvalues();
    element = row->add_values();
    element->set_intvalue(1);
    element = row->add_values();
    element->set_intvalue(2);

    EXPECT_EQ(ReturnStatus::OK, core->prepare(request));
    EXPECT_EQ(ReturnStatus::OK, core->execute());
    EXPECT_EQ(ReturnStatus::OK, core->getResults(response));
    EXPECT_NO_THROW(core->cleanup());
    EXPECT_EQ("0", response->results()[0].setofvalue().rowvalues(0).values(0).byteavalue());
    EXPECT_EQ("2", response->results()[0].setofvalue().rowvalues(0).values(1).byteavalue());
    EXPECT_EQ("1", response->results()[0].setofvalue().rowvalues(1).values(0).byteavalue());
    EXPECT_EQ("3", response->results()[0].setofvalue().rowvalues(1).values(1).byteavalue());
    EXPECT_EQ(PlcDataType::SETOF, response->results()[0].type());
}

TEST_F(RConvTest, RConvReturnSetofFromRegressOne) {
    plcontainer::CallRequest *request = new plcontainer::CallRequest();
    plcontainer::CallResponse *response = new plcontainer::CallResponse();
    SetOfData *data;
    ScalarData *element;
    CompositeData *row;

    PlcValue *arg1 = request->add_args();

    ProcSrc *src = request->mutable_proc();
    ReturnType *ret = request->mutable_rettype();

    ret->set_type(PlcDataType::TEXT);

    src->set_name("test");
    src->set_src(
        "x=''\nfor (row in a){\nx=paste(x,'#', row[1],'|',row[2],'|', row[3])\n}\nreturn(x)");
    arg1->set_name("a");
    arg1->set_type(PlcDataType::SETOF);

    data = arg1->mutable_setofvalue();
    data->add_columnnames();
    data->set_columnnames(0, "0");
    data->add_columnnames();
    data->set_columnnames(1, "1");
    data->add_columnnames();
    data->set_columnnames(2, "2");
    data->add_columntypes(PlcDataType::REAL);
    data->add_columntypes(PlcDataType::REAL);
    data->add_columntypes(PlcDataType::TEXT);

    // row 1
    row = data->add_rowvalues();
    element = row->add_values();
    element->set_realvalue(1);
    element = row->add_values();
    element->set_realvalue(1);
    element = row->add_values();
    element->set_stringvalue("a");

    // row 2
    row = data->add_rowvalues();
    element = row->add_values();
    element->set_realvalue(2);
    element = row->add_values();
    element->set_realvalue(2);
    element = row->add_values();
    element->set_stringvalue("b");

    // row 3
    row = data->add_rowvalues();
    element = row->add_values();
    element->set_realvalue(3);
    element = row->add_values();
    element->set_realvalue(3);
    element = row->add_values();
    element->set_stringvalue("c");

    EXPECT_EQ(ReturnStatus::OK, core->prepare(request));
    EXPECT_EQ(ReturnStatus::OK, core->execute());
    EXPECT_EQ(ReturnStatus::OK, core->getResults(response));
    EXPECT_NO_THROW(core->cleanup());
    EXPECT_EQ(PlcDataType::TEXT, response->results()[0].type());
}

TEST_F(RConvTest, RConvArgArrayWithOneArgsRegressTwo) {
    plcontainer::CallRequest *request = new plcontainer::CallRequest();
    plcontainer::CallResponse *response = new plcontainer::CallResponse();
    ArrayData *data;
    ScalarData *element;

    PlcValue *arg = request->add_args();

    ProcSrc *src = request->mutable_proc();
    ReturnType *ret = request->mutable_rettype();

    ret->set_type(PlcDataType::ARRAY);
    ret->add_subtypes(PlcDataType::INT);

    src->set_name("test");
    src->set_src("as.integer(a+1)");
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
    // EXPECT_EQ("a b c", response->results()[0].scalarvalue().stringvalue());
    EXPECT_EQ(PlcDataType::ARRAY, response->results()[0].type());

    EXPECT_EQ(ReturnStatus::OK, core->prepare(request));
    EXPECT_EQ(ReturnStatus::OK, core->execute());
    EXPECT_EQ(ReturnStatus::OK, core->getResults(response));
    EXPECT_NO_THROW(core->cleanup());
    // EXPECT_EQ("a b c", response->results()[0].scalarvalue().stringvalue());
    EXPECT_EQ(PlcDataType::ARRAY, response->results()[0].type());

    EXPECT_EQ(ReturnStatus::OK, core->prepare(request));
    EXPECT_EQ(ReturnStatus::OK, core->execute());
    EXPECT_EQ(ReturnStatus::OK, core->getResults(response));
    EXPECT_NO_THROW(core->cleanup());
    // EXPECT_EQ("a b c", response->results()[0].scalarvalue().stringvalue());
    EXPECT_EQ(PlcDataType::ARRAY, response->results()[0].type());
}

TEST_F(RConvTest, RConvArgArrayWithOneArgsRegressThree) {
    plcontainer::CallRequest *request = new plcontainer::CallRequest();
    plcontainer::CallResponse *response = new plcontainer::CallResponse();
    ArrayData *data;
    ScalarData *element;

    PlcValue *arg = request->add_args();

    ProcSrc *src = request->mutable_proc();
    ReturnType *ret = request->mutable_rettype();

    ret->set_type(PlcDataType::ARRAY);
    ret->add_subtypes(PlcDataType::REAL);

    src->set_name("test");
    src->set_src("a+1");
    arg->set_name("a");
    arg->set_type(PlcDataType::ARRAY);
    data = arg->mutable_arrayvalue();
    data->set_elementtype(PlcDataType::REAL);

    element = data->add_values();
    element->set_realvalue(1.23);
    element = data->add_values();
    element->set_realvalue(2.34);
    element = data->add_values();
    element->set_realvalue(3.45);

    EXPECT_EQ(ReturnStatus::OK, core->prepare(request));
    EXPECT_EQ(ReturnStatus::OK, core->execute());
    EXPECT_EQ(ReturnStatus::OK, core->getResults(response));
    EXPECT_NO_THROW(core->cleanup());
    // EXPECT_EQ("a b c", response->results()[0].scalarvalue().stringvalue());
    EXPECT_EQ(PlcDataType::ARRAY, response->results()[0].type());

    EXPECT_EQ(ReturnStatus::OK, core->prepare(request));
    EXPECT_EQ(ReturnStatus::OK, core->execute());
    EXPECT_EQ(ReturnStatus::OK, core->getResults(response));
    EXPECT_NO_THROW(core->cleanup());
    // EXPECT_EQ("a b c", response->results()[0].scalarvalue().stringvalue());
    EXPECT_EQ(PlcDataType::ARRAY, response->results()[0].type());

    EXPECT_EQ(ReturnStatus::OK, core->prepare(request));
    EXPECT_EQ(ReturnStatus::OK, core->execute());
    EXPECT_EQ(ReturnStatus::OK, core->getResults(response));
    EXPECT_NO_THROW(core->cleanup());
    // EXPECT_EQ("a b c", response->results()[0].scalarvalue().stringvalue());
    EXPECT_EQ(PlcDataType::ARRAY, response->results()[0].type());
}

TEST_F(RConvTest, RConvArgArrayWithOneArgsRegressFour) {
    plcontainer::CallRequest *request = new plcontainer::CallRequest();
    plcontainer::CallRequest *request1 = new plcontainer::CallRequest();
    plcontainer::CallRequest *request2 = new plcontainer::CallRequest();
    plcontainer::CallResponse *response = new plcontainer::CallResponse();
    ArrayData *data;
    ScalarData *element;
    PlcValue *arg;
    ProcSrc *src;
    ReturnType *ret;

    arg = request->add_args();

    src = request->mutable_proc();
    ret = request->mutable_rettype();

    ret->set_type(PlcDataType::ARRAY);
    ret->add_subtypes(PlcDataType::REAL);

    src->set_name("test");
    src->set_src("a+1");
    arg->set_name("a");
    arg->set_type(PlcDataType::ARRAY);
    data = arg->mutable_arrayvalue();
    data->set_elementtype(PlcDataType::REAL);

    element = data->add_values();
    element->set_realvalue(1.23);
    element = data->add_values();
    element->set_realvalue(2.34);
    element = data->add_values();
    element->set_realvalue(3.45);

    arg = request1->add_args();

    src = request1->mutable_proc();
    ret = request1->mutable_rettype();

    ret->set_type(PlcDataType::ARRAY);
    ret->add_subtypes(PlcDataType::REAL);

    src->set_name("test");
    src->set_src("a+1");
    arg->set_name("a");
    arg->set_type(PlcDataType::ARRAY);
    data = arg->mutable_arrayvalue();
    data->set_elementtype(PlcDataType::REAL);

    element = data->add_values();
    element->set_realvalue(1.23);
    element = data->add_values();
    element->set_realvalue(2.34);
    element = data->add_values();
    element->set_realvalue(3.45);

    arg = request2->add_args();

    src = request2->mutable_proc();
    ret = request2->mutable_rettype();

    ret->set_type(PlcDataType::ARRAY);
    ret->add_subtypes(PlcDataType::REAL);

    src->set_name("test");
    src->set_src("a+1");
    arg->set_name("a");
    arg->set_type(PlcDataType::ARRAY);
    data = arg->mutable_arrayvalue();
    data->set_elementtype(PlcDataType::REAL);

    element = data->add_values();
    element->set_realvalue(1.23);
    element = data->add_values();
    element->set_realvalue(2.34);
    element = data->add_values();
    element->set_realvalue(3.45);

    EXPECT_EQ(ReturnStatus::OK, core->prepare(request));
    EXPECT_EQ(ReturnStatus::OK, core->execute());
    EXPECT_EQ(ReturnStatus::OK, core->getResults(response));
    EXPECT_NO_THROW(core->cleanup());
    // EXPECT_EQ("a b c", response->results()[0].scalarvalue().stringvalue());
    EXPECT_EQ(PlcDataType::ARRAY, response->results()[0].type());

    EXPECT_EQ(ReturnStatus::OK, core->prepare(request1));
    EXPECT_EQ(ReturnStatus::OK, core->execute());
    EXPECT_EQ(ReturnStatus::OK, core->getResults(response));
    EXPECT_NO_THROW(core->cleanup());
    // EXPECT_EQ("a b c", response->results()[0].scalarvalue().stringvalue());
    EXPECT_EQ(PlcDataType::ARRAY, response->results()[0].type());

    EXPECT_EQ(ReturnStatus::OK, core->prepare(request2));
    EXPECT_EQ(ReturnStatus::OK, core->execute());
    EXPECT_EQ(ReturnStatus::OK, core->getResults(response));
    EXPECT_NO_THROW(core->cleanup());
    // EXPECT_EQ("a b c", response->results()[0].scalarvalue().stringvalue());
    EXPECT_EQ(PlcDataType::ARRAY, response->results()[0].type());
}

TEST_F(RConvTest, RConvArgArrayWithOneArgsRegressFive) {
    plcontainer::CallRequest *request = new plcontainer::CallRequest();
    plcontainer::CallRequest *request1 = new plcontainer::CallRequest();
    plcontainer::CallResponse *response = new plcontainer::CallResponse();
    plcontainer::CallResponse *response1 = new plcontainer::CallResponse();
    ArrayData *data;
    ScalarData *element;
    PlcValue *arg;
    ProcSrc *src;
    ReturnType *ret;

    arg = request->add_args();

    src = request->mutable_proc();
    ret = request->mutable_rettype();

    ret->set_type(PlcDataType::BYTEA);

    src->set_name("test");
    src->set_src("a");
    arg->set_name("a");
    arg->set_type(PlcDataType::ARRAY);
    data = arg->mutable_arrayvalue();
    data->set_elementtype(PlcDataType::INT);

    element = data->add_values();
    element->set_intvalue(123);
    element = data->add_values();
    element->set_intvalue(1);
    element = data->add_values();
    element->set_intvalue(7);

    EXPECT_EQ(ReturnStatus::OK, core->prepare(request));
    EXPECT_EQ(ReturnStatus::OK, core->execute());
    EXPECT_EQ(ReturnStatus::OK, core->getResults(response));
    EXPECT_NO_THROW(core->cleanup());
    // EXPECT_EQ("a b c", response->results()[0].scalarvalue().stringvalue());
    EXPECT_EQ(PlcDataType::BYTEA, response->results()[0].type());

    arg = request1->add_args();

    src = request1->mutable_proc();
    ret = request1->mutable_rettype();

    ret->set_type(PlcDataType::ARRAY);
    ret->add_subtypes(PlcDataType::INT);
    src->set_name("test");
    src->set_src("return (a)");
    arg->set_name("a");
    arg->set_type(PlcDataType::BYTEA);

    element = arg->mutable_scalarvalue();
    element->set_byteavalue(response->results()[0].scalarvalue().byteavalue());

    EXPECT_EQ(ReturnStatus::OK, core->prepare(request1));
    EXPECT_EQ(ReturnStatus::OK, core->execute());
    EXPECT_EQ(ReturnStatus::OK, core->getResults(response1));
    EXPECT_NO_THROW(core->cleanup());
    // EXPECT_EQ("a b c", response->results()[0].scalarvalue().stringvalue());
    EXPECT_EQ(PlcDataType::ARRAY, response1->results()[0].type());
}

TEST_F(RConvTest, RConvArgArrayWithOneArgsRegressSIX) {
    plcontainer::CallRequest *request = new plcontainer::CallRequest();
    plcontainer::CallResponse *response = new plcontainer::CallResponse();
    ArrayData *data;
    ScalarData *element;
    PlcValue *arg;
    ProcSrc *src;
    ReturnType *ret;

    src = request->mutable_proc();
    ret = request->mutable_rettype();

    ret->set_type(PlcDataType::SETOF);
    ret->add_subtypes(PlcDataType::TEXT);
    ret->add_subtypes(PlcDataType::REAL);

    src->set_name("test");
    src->set_src(
        "df <- data.frame(\n    date=date\n   ,returns=returns\n   "
        ",market_cap=market_cap\n   ,total_market_cap=total_market_cap\n   "
        ",benchmark_weight=benchmark_weight\n   ,benchmark_return=benchmark_return\n)\n  "
        "return(as.null(df))\n");

    arg = request->add_args();

    arg->set_name("date");
    arg->set_type(PlcDataType::ARRAY);
    data = arg->mutable_arrayvalue();
    data->set_elementtype(PlcDataType::TEXT);

    element = data->add_values();
    element->set_stringvalue("1984-01-04");
    element = data->add_values();
    element->set_stringvalue("1984-01-04");

    arg = request->add_args();

    arg->set_name("returns");
    arg->set_type(PlcDataType::ARRAY);
    data = arg->mutable_arrayvalue();
    data->set_elementtype(PlcDataType::REAL);

    element = data->add_values();
    element->set_realvalue(1);
    element = data->add_values();
    element->set_realvalue(1);

    arg = request->add_args();

    arg->set_name("market_cap");
    arg->set_type(PlcDataType::ARRAY);
    data = arg->mutable_arrayvalue();
    data->set_elementtype(PlcDataType::REAL);

    element = data->add_values();
    element->set_realvalue(1);
    element = data->add_values();
    element->set_realvalue(1);

    arg = request->add_args();

    arg->set_name("total_market_cap");
    arg->set_type(PlcDataType::ARRAY);
    data = arg->mutable_arrayvalue();
    data->set_elementtype(PlcDataType::REAL);

    element = data->add_values();
    element->set_realvalue(1);
    element = data->add_values();
    element->set_realvalue(1);

    arg = request->add_args();

    arg->set_name("benchmark_weight");
    arg->set_type(PlcDataType::ARRAY);
    data = arg->mutable_arrayvalue();
    data->set_elementtype(PlcDataType::REAL);

    element = data->add_values();
    element->set_realvalue(1);
    element = data->add_values();
    element->set_realvalue(1);

    arg = request->add_args();

    arg->set_name("benchmark_return");
    arg->set_type(PlcDataType::ARRAY);
    data = arg->mutable_arrayvalue();
    data->set_elementtype(PlcDataType::REAL);

    element = data->add_values();
    element->set_realvalue(1);
    element = data->add_values();
    element->set_realvalue(1);

    arg = request->add_args();

    arg->set_name("rollingwindowsize");
    arg->set_type(PlcDataType::INT);
    element = arg->mutable_scalarvalue();

    element->set_intvalue(100);

    EXPECT_EQ(ReturnStatus::OK, core->prepare(request));
    EXPECT_EQ(ReturnStatus::OK, core->execute());
    EXPECT_EQ(ReturnStatus::OK, core->getResults(response));
    EXPECT_NO_THROW(core->cleanup());
    // EXPECT_EQ("a b c", response->results()[0].scalarvalue().stringvalue());
    EXPECT_EQ(PlcDataType::SETOF, response->results()[0].type());
}

TEST_F(RConvTest, RConvArgINTRegressSEVEN) {
    plcontainer::CallRequest *request = new plcontainer::CallRequest();
    plcontainer::CallResponse *response = new plcontainer::CallResponse();

    PlcValue *arg = request->add_args();
    ScalarData *data;
    ProcSrc *src = request->mutable_proc();
    ReturnType *ret = request->mutable_rettype();

    ret->set_type(PlcDataType::INT);

    src->set_name("test");
    src->set_src("a <- 0\n for (i in 1:n){\n a <- a + 1\n}\n return (as.integer(a))\n");
    arg->set_name("n");
    arg->set_type(PlcDataType::INT);
    data = arg->mutable_scalarvalue();
    data->set_intvalue(10000000);

    EXPECT_EQ(ReturnStatus::OK, core->prepare(request));
    EXPECT_EQ(ReturnStatus::OK, core->execute());
    EXPECT_EQ(ReturnStatus::OK, core->getResults(response));
    EXPECT_NO_THROW(core->cleanup());
    // EXPECT_EQ(100, response->results()[0].scalarvalue().intvalue());
    EXPECT_EQ(PlcDataType::INT, response->results()[0].type());
}
