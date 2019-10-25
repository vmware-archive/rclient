/*------------------------------------------------------------------------------
 *
 * Copyright (c) 2019-Present Pivotal Software, Inc
 *
 *------------------------------------------------------------------------------
 */
#include "rtypeconverter.hh"

SEXP ConvertToSEXP::boolToSEXP(const bool &v) {
    SEXP result;
    PROTECT(result = ScalarLogical(v));
    return result;
}

SEXP ConvertToSEXP::intToSEXP(const int32_t &v) {
    SEXP result;
    PROTECT(result = ScalarInteger(v));
    return result;
}

SEXP ConvertToSEXP::realToSEXP(const double &v) {
    SEXP result;
    PROTECT(result = ScalarReal(v));
    return result;
}

SEXP ConvertToSEXP::textToSEXP(const std::string &v) {
    SEXP result;
    PROTECT(result = mkString(v.c_str()));
    return result;
}

SEXP ConvertToSEXP::byteaToSEXP(const std::string &v) {
    SEXP result;
    SEXP s, t, obj;
    int status;

    PROTECT(obj = NEW_RAW(v.size()));
    memcpy((char *)RAW(obj), v.data(), v.size());

    /*
     * Need to construct a call to
     * unserialize(rval)
     */
    PROTECT(t = s = allocList(2));
    SET_TYPEOF(s, LANGSXP);
    SETCAR(t, install("unserialize"));
    t = CDR(t);
    SETCAR(t, obj);

    PROTECT(result = R_tryEval(s, R_GlobalEnv, &status));
    if (status != 0) {
        // Error handling
    }

    UNPROTECT_PTR(s);
    UNPROTECT_PTR(t);
    UNPROTECT_PTR(obj);

    return result;
}

bool ConvertToProtoBuf::boolToProtoBuf(SEXP v) {
    bool res;
    switch (TYPEOF(v)) {
        case LGLSXP:
            res = asLogical(v);
            break;
        case INTSXP:
            res = (bool)asInteger(v) == 0 ? 0 : 1;
            break;
        case REALSXP:
            res = (bool)asReal(v) == 0 ? 0 : 1;
            break;
        default:
            break;
            // TODO: try-catch exception
    }
    return res;
}

int32_t ConvertToProtoBuf::intToProtoBuf(SEXP v) {
    int32_t res;
    switch (TYPEOF(v)) {
        case LGLSXP:
            res = asLogical(v);
            break;
        case INTSXP:
            res = asInteger(v);
            break;
        case REALSXP:
            res = (int32_t)asReal(v);
            break;
        default:
            break;
            // TODO: try-catch exception
    }
    return res;
}

double ConvertToProtoBuf::realToProtoBuf(SEXP v) {
    double res;
    switch (TYPEOF(v)) {
        case LGLSXP:
            res = asLogical(v);
            break;
        case INTSXP:
            res = asInteger(v);
            break;
        case REALSXP:
            res = asReal(v);
            break;
        default:
            break;
            // TODO: try-catch exception
    }
    return res;
}

std::string ConvertToProtoBuf::textToProtoBuf(SEXP v) {
    PrintValue(asChar(v));
    this->rLog.log(RServerLogLevel::LOGS, "get string is %s", CHAR(asChar(v)));
    return std::string(CHAR(asChar(v)));
}

std::string ConvertToProtoBuf::byteaToProtoBuf(SEXP v) {
    SEXP obj;
    SEXP s, t;
    int len, status;
    char *bytesArray;
    std::string result;

    /*
     * Need to construct a call to
     * serialize(rval, NULL)
     */
    PROTECT(t = s = allocList(3));
    SET_TYPEOF(s, LANGSXP);
    SETCAR(t, install("serialize"));
    t = CDR(t);
    SETCAR(t, v);
    t = CDR(t);
    SETCAR(t, R_NilValue);

    PROTECT(obj = R_tryEval(s, R_GlobalEnv, &status));
    if (status != 0) {
        this->rLog.log(RServerLogLevel::WARNINGS, "Cannot serialize object");
        // TODO: error handling
    }

    len = LENGTH(obj);
    this->rLog.log(RServerLogLevel::LOGS, "length is %d", len);
    bytesArray = (char *)RAW(obj);
    result = std::string(bytesArray, bytesArray + len);
    this->rLog.log(RServerLogLevel::LOGS, "result is %d", result.size());
    UNPROTECT_PTR(s);
    UNPROTECT_PTR(obj);

    return result;
}