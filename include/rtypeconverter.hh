/*------------------------------------------------------------------------------
 *
 * Copyright (c) 2019-Present Pivotal Software, Inc
 *
 *------------------------------------------------------------------------------
 */
#ifndef PLC_RTYPRCONVERTER_H
#define PLC_RTYPRCONVERTER_H

#include <vector>

#include "rutils.hh"

#include "plcontainer.grpc.pb.h"
#include "plcontainer.pb.h"

class ConvertToSEXP {
   public:
    ConvertToSEXP(RServerLog *rLog) { this->rLog = rLog; }
    SEXP boolToSEXP(const bool &v);
    SEXP intToSEXP(const int32_t &v);
    SEXP realToSEXP(const double &v);
    SEXP textToSEXP(const std::string &v);
    SEXP byteaToSEXP(const std::string &v);
    SEXP compositeToSEXP(const plcontainer::CompositeData &v);

    void setLogger(RServerLog *log) { this->rLog = log; }

   private:
    RServerLog *rLog;
    SEXP scalarToSEXP(const plcontainer::ScalarData &v);
};

class ConvertToProtoBuf {
   public:
    ConvertToProtoBuf(RServerLog *rLog) { this->rLog = rLog; }

    bool boolToProtoBuf(SEXP v);
    int32_t intToProtoBuf(SEXP v);
    double realToProtoBuf(SEXP v);
    std::string textToProtoBuf(SEXP v);
    std::string byteaToProtoBuf(SEXP v);
    void compositeToProtoBuf(SEXP v, std::vector<plcontainer::PlcDataType> &subTypes,
                             plcontainer::CompositeData *result);

    void setLogger(RServerLog *log) { this->rLog = log; }

   private:
    RServerLog *rLog;
    void scalarToProtobuf(SEXP v, plcontainer::PlcDataType type, plcontainer::ScalarData *result);
};

#endif  // PLC_RTYPRCONVERTER_H