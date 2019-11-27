/*------------------------------------------------------------------------------
 *
 * Copyright (c) 2019-Present Pivotal Software, Inc
 *
 *------------------------------------------------------------------------------
 */
#include "rtypeconverter.hh"

using namespace plcontainer;

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

    UNPROTECT_PTR(s);
    UNPROTECT_PTR(t);
    UNPROTECT_PTR(obj);

    if (status != 0) {
        this->rLog->log(RServerLogLevel::WARNINGS, "Could not convert bytea to R object");
    }

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
            this->rLog->log(RServerLogLevel::WARNINGS, "Could not convert R object to bool");
            break;
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
            this->rLog->log(RServerLogLevel::WARNINGS, "Could not convert R object to int");
            break;
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
            this->rLog->log(RServerLogLevel::WARNINGS, "Could not convert R object to real");
            break;
    }
    return res;
}

std::string ConvertToProtoBuf::textToProtoBuf(SEXP v) {
    PrintValue(asChar(v));
    this->rLog->log(RServerLogLevel::LOGS, "get string is %s", CHAR(asChar(v)));
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
        UNPROTECT_PTR(s);
        UNPROTECT_PTR(obj);
        this->rLog->log(RServerLogLevel::WARNINGS, "Cannot serialize object");
    }

    len = LENGTH(obj);
    this->rLog->log(RServerLogLevel::LOGS, "length is %d", len);
    bytesArray = (char *)RAW(obj);
    result = std::string(bytesArray, bytesArray + len);
    this->rLog->log(RServerLogLevel::LOGS, "result is %d", result.size());
    UNPROTECT_PTR(s);
    UNPROTECT_PTR(obj);

    return result;
}

SEXP ConvertToSEXP::compositeToSEXP(const plcontainer::CompositeData &v) {
    int i;
    SEXP result, r_name, c_names;

    PROTECT(result = NEW_LIST(v.values_size()));
    PROTECT(c_names = NEW_CHARACTER(v.values_size()));

    for (i = 0; i < v.values_size(); i++) {
        SEXP element;

        // Prepare column name
        this->rLog->log(RServerLogLevel::LOGS, "column %d, name is %s", i,
                        v.values()[i].name().c_str());
        SET_STRING_ELT(c_names, i, Rf_mkChar(v.values()[i].name().c_str()));

        // Prepare each element in UDT
        element = this->scalarToSEXP(v.values()[i]);
        SET_VECTOR_ELT(result, i, element);
        UNPROTECT_PTR(element);
    }
    // attach the column names
    setAttrib(result, R_NamesSymbol, c_names);
    UNPROTECT_PTR(c_names);  // names

    // attach row names - basically just the row number, zero based
    // TODO: check whether we need row name in data.frame
    PROTECT(r_name = allocVector(STRSXP, 1));
    SET_STRING_ELT(r_name, 0, Rf_mkChar("1"));

    setAttrib(result, R_RowNamesSymbol, r_name);
    UNPROTECT_PTR(r_name);  // row_names

    // UDT as considered as one row data.frame, which is followed by PLR
    setAttrib(result, R_ClassSymbol, mkString("data.frame"));

    // result must be protected
    return result;
}

SEXP ConvertToSEXP::scalarToSEXP(const plcontainer::ScalarData &v) {
    SEXP result;

    if (v.isnull()) {
        PROTECT(result = R_NilValue);
        return result;
    }

    switch (v.type()) {
        case PlcDataType::LOGICAL: {
            result = this->boolToSEXP(v.logicalvalue());
            break;
        }
        case PlcDataType::INT: {
            result = this->intToSEXP(v.intvalue());
            break;
        }
        case PlcDataType::REAL: {
            result = this->realToSEXP(v.realvalue());
            break;
        }
        case PlcDataType::TEXT: {
            result = this->textToSEXP(v.stringvalue());
            break;
        }
        case PlcDataType::BYTEA: {
            result = this->byteaToSEXP(v.byteavalue());
            break;
        }
        default: {
            this->rLog->log(RServerLogLevel::WARNINGS, "Unsupport type in value %s",
                            v.DebugString().c_str());
            PROTECT(result = R_NilValue);
        }
    }
    return result;
}

void ConvertToProtoBuf::scalarToProtobuf(SEXP v, PlcDataType type, ScalarData *result) {
    if (v == R_NilValue) {
        result->set_isnull(true);
        return;
    }

    switch (type) {
        case PlcDataType::LOGICAL: {
            result->set_logicalvalue(this->boolToProtoBuf(v));
            break;
        }
        case PlcDataType::INT: {
            result->set_intvalue(this->intToProtoBuf(v));
            break;
        }
        case PlcDataType::REAL: {
            result->set_realvalue(this->realToProtoBuf(v));
            break;
        }
        case PlcDataType::TEXT: {
            result->set_stringvalue(this->textToProtoBuf(v));
            break;
        }
        case PlcDataType::BYTEA: {
            result->set_byteavalue(this->byteaToProtoBuf(v));
            break;
        }
        default:
            result->set_isnull(true);
            this->rLog->log(RServerLogLevel::WARNINGS, "Unsupport type from R to GPDB");
    }
}

void ConvertToProtoBuf::compositeToProtoBuf(SEXP v, std::vector<PlcDataType> &subTypes,
                                            CompositeData *result) {
    SEXP dfcol;

    if (!isFrame(v)) {
        this->rLog->log(RServerLogLevel::WARNINGS, "Returned R Object must be Data.Frame");
    }

    // Convert each column to a subtype in GPDB UDT based on sub-type
    this->rLog->log(RServerLogLevel::LOGS, "Start to add memeber in UDT, total size is %d",
                    subTypes.size());
    for (int i = 0; i < (int)subTypes.size(); i++) {
        PROTECT(dfcol = VECTOR_ELT(v, i));
        if (isFactor(dfcol)) {
            SEXP c;
            if (INTEGER(dfcol)[i] != NA_INTEGER) {
                c = Rf_asCharacterFactor(dfcol);
            } else {
                c = R_NilValue;
            }
            this->scalarToProtobuf(c, subTypes[i], result->add_values());
        } else {
            this->scalarToProtobuf(dfcol, subTypes[i], result->add_values());
        }
        UNPROTECT_PTR(dfcol);
    }
}
