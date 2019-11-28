/*------------------------------------------------------------------------------
 *
 * Copyright (c) 2019-Present Pivotal Software, Inc
 *
 *------------------------------------------------------------------------------
 */
#ifndef PLC_RCALL_H
#define PLC_RCALL_H

#include <iostream>
#include <string>
#include <exception>
#include <vector>

#include "rutils.hh"

#include "plcontainer.pb.h"
#include "plcontainer.grpc.pb.h"

#include "rtypeconverter.hh"

namespace plcontainer {

class rParseErrorException : public std::exception {
   public:
    rParseErrorException(const std::string &errMsg) {
        this->errMsg = "Error evaluating function " + errMsg;
    }

    virtual const char *what() const noexcept override { return this->errMsg.c_str(); }

   private:
    std::string errMsg;
};

class rArgumentErrorException : public std::exception {
   public:
    rArgumentErrorException(const std::string &errMsg) {
        this->errMsg = "The input argument has an error " + errMsg;
    }

    virtual const char *what() const noexcept override { return this->errMsg.c_str(); }

   private:
    std::string errMsg;
};

class userFunction {
   public:
    userFunction() {}

    void setFunctionName(const std::string &name) { this->functionName = name; }

    const std::string &getFunctionName() const { return this->functionName; }

    void setFunctionBody(const std::string &body) { this->functionBody = body; }

    const std::string &getFunctionBody() const { return this->functionBody; }

    void addFunctionArgsName(const std::string &argName) {
        return this->functionargs.push_back(argName);
    }

    const std::vector<std::string> &getFunctionArgsName() const { return this->functionargs; }

    void cleanFunctionArgNames() { this->functionargs.clear(); }

   private:
    std::string functionName;
    std::string functionBody;
    std::vector<std::string> functionargs;
};

enum class ReturnStatus {
    OK = 1,
    FAIL,
    UNKNOWN = 99,
};

class PlcRuntime {
   public:
    virtual ReturnStatus init() = 0;
    virtual ReturnStatus prepare(const CallRequest *callRequest) = 0;
    virtual ReturnStatus execute() = 0;
    virtual ReturnStatus getResults(CallResponse *results) = 0;
    virtual void cleanup() = 0;

    virtual void setLogger(RServerLog *log) = 0;
    virtual ~PlcRuntime() {}
};

class RCoreRuntime : public PlcRuntime {
   public:
    explicit RCoreRuntime(RServerLog *rLog)
        : rCode(nullptr), rArgument(nullptr), rFunc(nullptr), rResults(nullptr) {
        this->runType = R;
        this->rLog = rLog;
    };

    virtual ReturnStatus init() override;

    virtual ReturnStatus prepare(const CallRequest *callRequest) override;

    virtual ReturnStatus execute() override;

    virtual ReturnStatus getResults(CallResponse *results) override;

    virtual void cleanup() override;

    void setLogger(RServerLog *log) { this->rLog = log; }

    virtual ~RCoreRuntime() {};

   protected:
    SEXP rCode;
    SEXP rArgument;
    SEXP rFunc;
    SEXP rResults;
    PlcRuntimeType runType;
    PlcDataType returnType;
    std::vector<PlcDataType> returnSubType;
    RServerLog *rLog;
    // TODO: add cache for further requests

    void loadRCmd(const std::string &cmd);
    std::string getLoadSelfRefCmd();
    std::string getRsourceCode(const struct userFunction &userCode);
    ReturnStatus parseRCode(const std::string &code);
    ReturnStatus setArgumentValues(const CallRequest *callRequest);
    ReturnStatus prepareRFunctionSEXP(const CallRequest *callRequest);
};

}  // namespace plcontainer

#define UNUSED __attribute__((unused))

#endif /* PLC_RCALL_H */
