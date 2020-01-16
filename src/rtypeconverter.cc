/*------------------------------------------------------------------------------
 *
 * Copyright (c) 2019-Present Pivotal Software, Inc
 *
 *------------------------------------------------------------------------------
 */
#include "rtypeconverter.hh"

SEXP ConvertToSEXP::boolToSEXP(const bool v) {
    SEXP result;
    PROTECT(result = ScalarLogical(v));
    return result;
}

SEXP ConvertToSEXP::intToSEXP(const int32_t v) {
    SEXP result;
    PROTECT(result = ScalarInteger(v));
    return result;
}

SEXP ConvertToSEXP::realToSEXP(const double v) {
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
        case plcontainer::PlcDataType::LOGICAL: {
            result = this->boolToSEXP(v.logicalvalue());
            break;
        }
        case plcontainer::PlcDataType::INT: {
            result = this->intToSEXP(v.intvalue());
            break;
        }
        case plcontainer::PlcDataType::REAL: {
            result = this->realToSEXP(v.realvalue());
            break;
        }
        case plcontainer::PlcDataType::TEXT: {
            result = this->textToSEXP(v.stringvalue());
            break;
        }
        case plcontainer::PlcDataType::BYTEA: {
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

void ConvertToProtoBuf::scalarToProtobuf(SEXP v, plcontainer::PlcDataType type,
                                         plcontainer::ScalarData *result) {
    if (v == R_NilValue) {
        result->set_isnull(true);
        return;
    }

    switch (type) {
        case plcontainer::PlcDataType::LOGICAL: {
            result->set_logicalvalue(this->boolToProtoBuf(v));
            break;
        }
        case plcontainer::PlcDataType::INT: {
            result->set_intvalue(this->intToProtoBuf(v));
            break;
        }
        case plcontainer::PlcDataType::REAL: {
            result->set_realvalue(this->realToProtoBuf(v));
            break;
        }
        case plcontainer::PlcDataType::TEXT: {
            result->set_stringvalue(this->textToProtoBuf(v));
            break;
        }
        case plcontainer::PlcDataType::BYTEA: {
            result->set_byteavalue(this->byteaToProtoBuf(v));
            break;
        }
        default:
            result->set_isnull(true);
            this->rLog->log(RServerLogLevel::WARNINGS, "Unsupport type from R to GPDB");
    }
}

void ConvertToProtoBuf::compositeToProtoBuf(SEXP v, std::vector<plcontainer::PlcDataType> &subTypes,
                                            plcontainer::CompositeData *result) {
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

SEXP ConvertToSEXP::arrayToSEXP(const plcontainer::ArrayData &v) {
    SEXP result, array_dims;
    int length = v.values_size();

    result = this->allocRVector(v.elementtype(), length);
    switch (v.elementtype()) {
        case plcontainer::PlcDataType::LOGICAL: {
            for (int i = 0; i < length; i++) {
                if (!v.values()[i].isnull()) {
                    LOGICAL_DATA(result)[i] = v.values()[i].logicalvalue();
                } else {
                    LOGICAL_DATA(result)[i] = NA_LOGICAL;
                }
            }
        } break;
        case plcontainer::PlcDataType::INT: {
            for (int i = 0; i < length; i++) {
                if (!v.values()[i].isnull()) {
                    INTEGER_DATA(result)[i] = v.values()[i].intvalue();
                } else {
                    INTEGER_DATA(result)[i] = NA_INTEGER;
                }
            }
        } break;
        case plcontainer::PlcDataType::REAL: {
            for (int i = 0; i < length; i++) {
                if (!v.values()[i].isnull()) {
                    NUMERIC_DATA(result)[i] = v.values()[i].realvalue();
                } else {
                    NUMERIC_DATA(result)[i] = NA_REAL;
                }
            }
        } break;
        case plcontainer::PlcDataType::BYTEA: {
            for (int i = 0; i < length; i++) {
                if (!v.values()[i].isnull()) {
                    SET_STRING_ELT(result, i,
                                   COPY_TO_USER_STRING(v.values()[i].byteavalue().data()));
                } else {
                    SET_STRING_ELT(result, i, NA_STRING);
                }
            }
        } break;
        case plcontainer::PlcDataType::TEXT: {
            for (int i = 0; i < length; i++) {
                if (!v.values()[i].isnull()) {
                    SET_STRING_ELT(result, i,
                                   COPY_TO_USER_STRING(v.values()[i].stringvalue().c_str()));
                } else {
                    SET_STRING_ELT(result, i, NA_STRING);
                }
            }
        } break;
        default:
            this->rLog->log(RServerLogLevel::WARNINGS,
                            "Unsupport type in array argument, which is %d", v.elementtype());
            PROTECT(result = R_NilValue);
            break;
    }

    // Currently only support 1-D array
    PROTECT(array_dims = allocVector(INTSXP, 1));
    INTEGER_DATA(array_dims)[0] = length;
    setAttrib(result, R_DimSymbol, array_dims);
    UNPROTECT_PTR(array_dims);

    return result;
}

void ConvertToProtoBuf::arrayToProtoBuf(SEXP v, plcontainer::PlcDataType type,
                                        plcontainer::ArrayData *result) {
    int length = Rf_length(v);

    // convert R vector into Protobuf directly
    switch (TYPEOF(v)) {
        case LGLSXP: {
            for (int i = 0; i < length; i++) {
                plcontainer::ScalarData *element = result->add_values();
                if (LOGICAL(v)[i] != NA_LOGICAL) {
                    switch (type) {
                        case plcontainer::PlcDataType::LOGICAL: {
                            element->set_logicalvalue(LOGICAL(v)[i]);
                        } break;
                        case plcontainer::PlcDataType::INT: {
                            element->set_intvalue((int32_t)LOGICAL(v)[i]);
                        } break;
                        case plcontainer::PlcDataType::REAL: {
                            element->set_realvalue((double)LOGICAL(v)[i]);
                        } break;
                        case plcontainer::PlcDataType::TEXT: {
                            element->set_stringvalue(std::to_string(LOGICAL(v)[i]));
                        } break;
                        case plcontainer::PlcDataType::BYTEA: {
                            element->set_byteavalue(std::to_string(LOGICAL(v)[i]));
                        } break;
                        default:
                            this->rLog->log(RServerLogLevel::WARNINGS, "Unsupport type in value %d",
                                            type);
                            break;
                    }
                } else {
                    element->set_isnull(true);
                }
            }
        } break;
        case INTSXP: {
            for (int i = 0; i < length; i++) {
                plcontainer::ScalarData *element = result->add_values();
                if (INTEGER(v)[i] != NA_INTEGER) {
                    switch (type) {
                        case plcontainer::PlcDataType::LOGICAL: {
                            element->set_logicalvalue((bool)INTEGER(v)[i] == 0 ? 0 : 1);
                        } break;
                        case plcontainer::PlcDataType::INT: {
                            element->set_intvalue(INTEGER(v)[i]);
                        } break;
                        case plcontainer::PlcDataType::REAL: {
                            element->set_realvalue((double)INTEGER(v)[i]);
                        } break;
                        case plcontainer::PlcDataType::TEXT: {
                            element->set_stringvalue(std::to_string(INTEGER(v)[i]));
                        } break;
                        case plcontainer::PlcDataType::BYTEA: {
                            element->set_byteavalue(std::to_string(INTEGER(v)[i]));
                        } break;
                        default:
                            this->rLog->log(RServerLogLevel::WARNINGS, "Unsupport type in value %d",
                                            type);
                            break;
                    }
                } else {
                    element->set_isnull(true);
                }
            }
        } break;
        case REALSXP: {
            for (int i = 0; i < length; i++) {
                plcontainer::ScalarData *element = result->add_values();
                if (REAL(v)[i] != NA_REAL) {
                    switch (type) {
                        case plcontainer::PlcDataType::LOGICAL: {
                            element->set_logicalvalue((bool)REAL(v)[i] == 0 ? 0 : 1);
                        } break;
                        case plcontainer::PlcDataType::INT: {
                            element->set_intvalue((int32_t)REAL(v)[i]);
                        } break;
                        case plcontainer::PlcDataType::REAL: {
                            element->set_realvalue(REAL(v)[i]);
                        } break;
                        case plcontainer::PlcDataType::TEXT: {
                            element->set_stringvalue(std::to_string(REAL(v)[i]));
                        } break;
                        case plcontainer::PlcDataType::BYTEA: {
                            element->set_byteavalue(std::to_string(REAL(v)[i]));
                        } break;
                        default:
                            this->rLog->log(RServerLogLevel::WARNINGS, "Unsupport type in value %d",
                                            type);
                            break;
                    }
                } else {
                    element->set_isnull(true);
                }
            }
        } break;
        case STRSXP: {
            for (int i = 0; i < length; i++) {
                const char *value = CHAR(STRING_ELT(v, i));
                plcontainer::ScalarData *element = result->add_values();
                if (STRING_ELT(v, i) != NA_STRING && value != NULL) {
                    switch (type) {
                        case plcontainer::PlcDataType::TEXT: {
                            element->set_stringvalue(std::string(value));
                        } break;
                        case plcontainer::PlcDataType::BYTEA: {
                            element->set_byteavalue(std::string(value));
                        } break;
                        default:
                            this->rLog->log(RServerLogLevel::WARNINGS, "Unsupport type in value %d",
                                            type);
                            break;
                    }
                } else {
                    element->set_isnull(true);
                }
            }
        } break;
        default:
            this->rLog->log(RServerLogLevel::WARNINGS, "Unsupport type in value %d", type);
            break;
    }
}

SEXP ConvertToSEXP::allocRVector(const plcontainer::PlcDataType type, int length) {
    SEXP result;

    switch (type) {
        case plcontainer::PlcDataType::LOGICAL:
            PROTECT(result = NEW_LOGICAL(length));
            break;
        case plcontainer::PlcDataType::INT:
            PROTECT(result = NEW_INTEGER(length));
            break;
        case plcontainer::PlcDataType::REAL:
            PROTECT(result = NEW_NUMERIC(length));
            break;
        // BYTEA array is considered as string
        case plcontainer::PlcDataType::BYTEA:
        case plcontainer::PlcDataType::TEXT:
            PROTECT(result = NEW_CHARACTER(length));
            break;
        default:
            this->rLog->log(RServerLogLevel::WARNINGS, "Unsupport type in value %d", type);
            PROTECT(result = R_NilValue);
            break;
    }
    return result;
}