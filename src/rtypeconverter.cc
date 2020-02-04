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

    if (status != 0) {
        this->rLog->log(RServerLogLevel::ERRORS, "Could not convert bytea to R object");
    }

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
            this->rLog->log(RServerLogLevel::ERRORS, "Could not convert R object to bool");
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
            this->rLog->log(RServerLogLevel::ERRORS, "Could not convert R object to int");
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
            this->rLog->log(RServerLogLevel::ERRORS, "Could not convert R object to real");
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
        this->rLog->log(RServerLogLevel::ERRORS, "Cannot serialize object");
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
            this->rLog->log(RServerLogLevel::ERRORS, "Unsupport type in value %s",
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
            this->rLog->log(RServerLogLevel::ERRORS, "Unsupport type from R to GPDB");
    }
}

void ConvertToProtoBuf::compositeToProtoBuf(SEXP v, std::vector<plcontainer::PlcDataType> &subTypes,
                                            plcontainer::CompositeData *result) {
    SEXP dfcol;

    if (!isFrame(v)) {
        this->rLog->log(RServerLogLevel::ERRORS, "Returned R Object must be Data.Frame");
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
            this->rLog->log(RServerLogLevel::ERRORS,
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
                            this->rLog->log(RServerLogLevel::ERRORS, "Unsupport type in value %d",
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
                            this->rLog->log(RServerLogLevel::ERRORS, "Unsupport type in value %d",
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
                            this->rLog->log(RServerLogLevel::ERRORS, "Unsupport type in value %d",
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
                            this->rLog->log(RServerLogLevel::ERRORS, "Unsupport type in value %d",
                                            type);
                            break;
                    }
                } else {
                    element->set_isnull(true);
                }
            }
        } break;
        default:
            this->rLog->log(RServerLogLevel::ERRORS, "Unsupport type in value %d", type);
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
            this->rLog->log(RServerLogLevel::ERRORS, "Unsupport type in value %d", type);
            PROTECT(result = R_NilValue);
            break;
    }
    return result;
}

SEXP ConvertToSEXP::setofToSEXP(const plcontainer::SetOfData &v) {
    SEXP result = R_NilValue;
    SEXP cnames = R_NilValue;
    SEXP rnames = R_NilValue;
    int columnLen = v.columntypes_size();
    int32_t rowLen = v.rowvalues_size();

    // init a new list with number of columns
    PROTECT(result = NEW_LIST(columnLen));
    // init a new string vector with number of columns
    PROTECT(cnames = NEW_CHARACTER(columnLen));

    for (int i = 0; i < columnLen; i++) {
        SEXP columnData = R_NilValue;
        // Set column name
        SET_STRING_ELT(cnames, i, mkChar(v.columnnames(i).c_str()));

        // Init a new vector for this column
        columnData = this->allocRVector(v.columntypes(i), rowLen);

        switch (v.columntypes(i)) {
            case plcontainer::PlcDataType::LOGICAL: {
                for (int j = 0; j < rowLen; j++) {
                    if (!v.rowvalues(j).values(i).isnull()) {
                        LOGICAL_DATA(columnData)[j] = v.rowvalues(j).values(i).logicalvalue();
                    } else {
                        LOGICAL_DATA(columnData)[j] = NA_LOGICAL;
                    }
                }
            } break;
            case plcontainer::PlcDataType::INT: {
                for (int j = 0; j < rowLen; j++) {
                    if (!v.rowvalues(j).values(i).isnull()) {
                        INTEGER_DATA(columnData)[j] = v.rowvalues(j).values(i).intvalue();
                    } else {
                        INTEGER_DATA(columnData)[j] = NA_INTEGER;
                    }
                }
            } break;
            case plcontainer::PlcDataType::REAL: {
                for (int j = 0; j < rowLen; j++) {
                    if (!v.rowvalues(j).values(i).isnull()) {
                        NUMERIC_DATA(columnData)[j] = v.rowvalues(j).values(i).realvalue();
                    } else {
                        NUMERIC_DATA(columnData)[j] = NA_REAL;
                    }
                }
            } break;
            case plcontainer::PlcDataType::BYTEA: {
                for (int j = 0; j < rowLen; j++) {
                    if (!v.rowvalues(j).values(i).isnull()) {
                        SET_STRING_ELT(
                            columnData, j,
                            COPY_TO_USER_STRING(v.rowvalues(j).values(i).byteavalue().data()));
                    } else {
                        SET_STRING_ELT(columnData, j, NA_STRING);
                    }
                }
            } break;
            case plcontainer::PlcDataType::TEXT: {
                for (int j = 0; j < rowLen; j++) {
                    if (!v.rowvalues(j).values(i).isnull()) {
                        SET_STRING_ELT(
                            columnData, j,
                            COPY_TO_USER_STRING(v.rowvalues(j).values(i).stringvalue().c_str()));
                    } else {
                        SET_STRING_ELT(columnData, j, NA_STRING);
                    }
                }
            } break;
            default:
                this->rLog->log(RServerLogLevel::ERRORS,
                                "Unsupport type in array argument, which is %d", v.columntypes(i));
                PROTECT(result = R_NilValue);
                break;
        }

        SET_VECTOR_ELT(result, i, columnData);
        UNPROTECT_PTR(columnData);
    }

    // Attach the column names
    setAttrib(result, R_NamesSymbol, cnames);

    // attach row names, we use row number here, start from 1, it is optional
    rnames = this->allocRVector(plcontainer::PlcDataType::TEXT, rowLen);
    for (int i = 0; i < rowLen; i++) {
        SET_STRING_ELT(rnames, i, COPY_TO_USER_STRING(std::to_string(i + 1).c_str()));
    }
    setAttrib(result, R_RowNamesSymbol, rnames);
    // The setof is converted as a data.frame structure
    setAttrib(result, R_ClassSymbol, mkString("data.frame"));
    UNPROTECT_PTR(cnames);
    UNPROTECT_PTR(rnames);

    return result;
}

void ConvertToProtoBuf::dataframeToSetof(SEXP frame,
                                         std::vector<plcontainer::PlcDataType> &subTypes,
                                         plcontainer::SetOfData *result) {
    int32_t rowlength;

    if (Rf_length(frame) != (int)subTypes.size()) {
        this->rLog->log(RServerLogLevel::ERRORS,
                        "The number of column in data.frame (%d) does not match the number of "
                        "column in returned type (%d)",
                        Rf_length(frame), subTypes.size());
    }

    // Get the number of rows, List is considered as one row data frame
    if (isFrame(frame)) {
        SEXP firstColumn;
        PROTECT(firstColumn = VECTOR_ELT(frame, 0));
        rowlength = Rf_length(firstColumn);
        UNPROTECT_PTR(firstColumn);
    } else {
        rowlength = 1;
    }

    // Init the result first via add_*()
    for (int32_t r = 0; r < rowlength; r++) {
        plcontainer::CompositeData *cdata;
        cdata = result->add_rowvalues();
        for (int c = 0; c < (int)subTypes.size(); c++) {
            cdata->add_values();
        }
    }

    // extract each column from data.frame
    for (int i = 0; i < (int)subTypes.size(); i++) {
        SEXP column;

        PROTECT(column = VECTOR_ELT(frame, i));

        if (isFactor(column)) {
            SEXP f;
            PROTECT(f = Rf_asCharacterFactor(column));
            // We need to convert factor column to string
            this->dataframeColumnStoreToRowStore(f, rowlength, i, result,
                                                 plcontainer::PlcDataType::TEXT);
            UNPROTECT_PTR(f);
        } else {
            // Check whether the code can go fast path
            switch (subTypes[i]) {
                case plcontainer::PlcDataType::LOGICAL: {
                    if (IS_LOGICAL(column)) {
                        this->dataframeColumnStoreToRowStoreFastPath(
                            column, rowlength, i, result, plcontainer::PlcDataType::LOGICAL);
                    } else {
                        // test failure, go normal path
                        this->dataframeColumnStoreToRowStore(column, rowlength, i, result,
                                                             subTypes[i]);
                    }
                } break;
                case plcontainer::PlcDataType::INT: {
                    if (IS_INTEGER(column)) {
                        this->dataframeColumnStoreToRowStoreFastPath(column, rowlength, i, result,
                                                                     plcontainer::PlcDataType::INT);
                    } else {
                        // test failure, go normal path
                        this->dataframeColumnStoreToRowStore(column, rowlength, i, result,
                                                             subTypes[i]);
                    }
                } break;
                case plcontainer::PlcDataType::REAL: {
                    if (IS_NUMERIC(column) || IS_INTEGER(column)) {
                        this->dataframeColumnStoreToRowStoreFastPath(
                            column, rowlength, i, result, plcontainer::PlcDataType::REAL);
                    } else {
                        // test failure, go normal path
                        this->dataframeColumnStoreToRowStore(column, rowlength, i, result,
                                                             subTypes[i]);
                    }
                } break;
                default:
                    this->dataframeColumnStoreToRowStore(column, rowlength, i, result, subTypes[i]);
                    break;
            }
        }
        UNPROTECT_PTR(column);
    }
}

void ConvertToProtoBuf::dataframeColumnStoreToRowStoreFastPath(SEXP column, int32_t rowlength,
                                                               int columnId,
                                                               plcontainer::SetOfData *result,
                                                               plcontainer::PlcDataType cType) {
    switch (cType) {
        case plcontainer::PlcDataType::LOGICAL: {
            for (int32_t i = 0; i < rowlength; i++) {
                if (LOGICAL_DATA(column)[i] != NA_LOGICAL) {
                    result->mutable_rowvalues(i)->mutable_values(columnId)->set_logicalvalue(
                        LOGICAL_DATA(column)[i]);
                } else {
                    result->mutable_rowvalues(i)->mutable_values(columnId)->set_isnull(true);
                }
            }
        } break;
        case plcontainer::PlcDataType::INT: {
            for (int32_t i = 0; i < rowlength; i++) {
                if (INTEGER_DATA(column)[i] != NA_INTEGER) {
                    result->mutable_rowvalues(i)->mutable_values(columnId)->set_intvalue(
                        INTEGER_DATA(column)[i]);
                } else {
                    result->mutable_rowvalues(i)->mutable_values(columnId)->set_isnull(true);
                }
            }
        }
        case plcontainer::PlcDataType::REAL: {
            if (IS_INTEGER(column)) {
                for (int32_t i = 0; i < rowlength; i++) {
                    if (INTEGER_DATA(column)[i] != NA_INTEGER) {
                        result->mutable_rowvalues(i)->mutable_values(columnId)->set_realvalue(
                            (double)INTEGER_DATA(column)[i]);
                    } else {
                        result->mutable_rowvalues(i)->mutable_values(columnId)->set_isnull(true);
                    }
                }
            } else {
                for (int32_t i = 0; i < rowlength; i++) {
                    if (NUMERIC_DATA(column)[i] != NA_REAL) {
                        result->mutable_rowvalues(i)->mutable_values(columnId)->set_realvalue(
                            NUMERIC_DATA(column)[i]);
                    } else {
                        result->mutable_rowvalues(i)->mutable_values(columnId)->set_isnull(true);
                    }
                }
            }
        }
        default:
            break;
    }
}

void ConvertToProtoBuf::dataframeColumnStoreToRowStore(SEXP column, int32_t rowlength, int columnId,
                                                       plcontainer::SetOfData *result,
                                                       plcontainer::PlcDataType cType) {
    switch (TYPEOF(column)) {
        case LGLSXP: {
            for (int32_t i = 0; i < rowlength; i++) {
                plcontainer::ScalarData *element =
                    result->mutable_rowvalues(i)->mutable_values(columnId);
                if (LOGICAL(column)[i] != NA_LOGICAL) {
                    switch (cType) {
                        case plcontainer::PlcDataType::LOGICAL: {
                            this->rLog->log(RServerLogLevel::LOGS, "return type is bool");
                            element->set_logicalvalue(LOGICAL(column)[i]);
                        } break;
                        case plcontainer::PlcDataType::INT: {
                            this->rLog->log(RServerLogLevel::LOGS, "return type is int");
                            element->set_intvalue((int32_t)LOGICAL(column)[i]);
                        } break;
                        case plcontainer::PlcDataType::REAL: {
                            element->set_realvalue((double)LOGICAL(column)[i]);
                        } break;
                        case plcontainer::PlcDataType::TEXT: {
                            element->set_stringvalue(std::to_string(LOGICAL(column)[i]));
                        } break;
                        case plcontainer::PlcDataType::BYTEA: {
                            element->set_byteavalue(std::to_string(LOGICAL(column)[i]));
                        } break;
                        default:
                            this->rLog->log(RServerLogLevel::ERRORS, "Unsupport type in value %d",
                                            cType);
                            break;
                    }
                } else {
                    element->set_isnull(true);
                }
            }
        } break;
        case INTSXP: {
            for (int32_t i = 0; i < rowlength; i++) {
                plcontainer::ScalarData *element =
                    result->mutable_rowvalues(i)->mutable_values(columnId);
                if (INTEGER(column)[i] != NA_INTEGER) {
                    switch (cType) {
                        case plcontainer::PlcDataType::LOGICAL: {
                            element->set_logicalvalue(INTEGER(column)[i] == 0 ? false : true);
                        } break;
                        case plcontainer::PlcDataType::INT: {
                            element->set_intvalue(INTEGER(column)[i]);
                        } break;
                        case plcontainer::PlcDataType::REAL: {
                            element->set_realvalue((double)INTEGER(column)[i]);
                        } break;
                        case plcontainer::PlcDataType::TEXT: {
                            element->set_stringvalue(std::to_string(INTEGER(column)[i]));
                        } break;
                        case plcontainer::PlcDataType::BYTEA: {
                            element->set_byteavalue(std::to_string(INTEGER(column)[i]));
                        } break;
                        default:
                            this->rLog->log(RServerLogLevel::ERRORS, "Unsupport type in value %d",
                                            cType);
                            break;
                    }
                } else {
                    element->set_isnull(true);
                }
            }
        } break;
        case REALSXP: {
            for (int32_t i = 0; i < rowlength; i++) {
                plcontainer::ScalarData *element =
                    result->mutable_rowvalues(i)->mutable_values(columnId);
                if (REAL(column)[i] != NA_REAL) {
                    switch (cType) {
                        case plcontainer::PlcDataType::LOGICAL: {
                            element->set_logicalvalue((bool)REAL(column)[i] == 0 ? 0 : 1);
                        } break;
                        case plcontainer::PlcDataType::INT: {
                            element->set_intvalue((int32_t)REAL(column)[i]);
                        } break;
                        case plcontainer::PlcDataType::REAL: {
                            element->set_realvalue(REAL(column)[i]);
                        } break;
                        case plcontainer::PlcDataType::TEXT: {
                            element->set_stringvalue(std::to_string(REAL(column)[i]));
                        } break;
                        case plcontainer::PlcDataType::BYTEA: {
                            element->set_byteavalue(std::to_string(REAL(column)[i]));
                        } break;
                        default:
                            this->rLog->log(RServerLogLevel::ERRORS, "Unsupport type in value %d",
                                            cType);
                            break;
                    }
                } else {
                    element->set_isnull(true);
                }
            }
        } break;
        case STRSXP: {
            for (int32_t i = 0; i < rowlength; i++) {
                const char *value = CHAR(STRING_ELT(column, i));
                plcontainer::ScalarData *element =
                    result->mutable_rowvalues(i)->mutable_values(columnId);
                if (STRING_ELT(column, i) != NA_STRING && value != NULL) {
                    switch (cType) {
                        case plcontainer::PlcDataType::TEXT: {
                            element->set_stringvalue(std::string(value));
                        } break;
                        case plcontainer::PlcDataType::BYTEA: {
                            element->set_byteavalue(std::string(value));
                        } break;
                        default:
                            this->rLog->log(RServerLogLevel::ERRORS, "Unsupport type in value %d",
                                            cType);
                            break;
                    }
                } else {
                    element->set_isnull(true);
                }
            }
        } break;
        default:
            this->rLog->log(RServerLogLevel::ERRORS, "Unsupport type in value %d", cType);
            break;
    }
}

void ConvertToProtoBuf::matrixToSetof(SEXP mx, std::vector<plcontainer::PlcDataType> &subTypes,
                                      plcontainer::SetOfData *result) {

    int columnLength = subTypes.size();
    int rowLength;

    if (isMatrix(mx)) {
        // get the n*m details from matrix
        if (columnLength == ncols(mx)) {
            rowLength = nrows(mx);
        } else {
            this->rLog->log(RServerLogLevel::ERRORS,
                            "the column length %d of R object is different with column length %d "
                            "of return type in matrix",
                            ncols(mx), columnLength);
        }

    } else {
        // vector is considered as a one row matrix
        if (columnLength == Rf_length(mx)) {
            rowLength = 1;
        } else {
            this->rLog->log(RServerLogLevel::ERRORS,
                            "the column length %d of R object is different with column length %d "
                            "of return type in vector",
                            Rf_length(mx), columnLength);
        }
    }

    // Init the result first via add_*()

    for (int r = 0; r < rowLength; r++) {
        plcontainer::CompositeData *cdata;
        cdata = result->add_rowvalues();
        for (int c = 0; c < columnLength; c++) {
            cdata->add_values();
        }
    }

    for (int i = 0; i < columnLength; i++) {
        switch (subTypes[i]) {
            case plcontainer::PlcDataType::LOGICAL: {
                switch (TYPEOF(mx)) {
                    case LGLSXP: {
                        for (int j = 0; j < rowLength; j++) {
                            plcontainer::ScalarData *element =
                                result->mutable_rowvalues(j)->mutable_values(i);
                            if (LOGICAL(mx)[rowLength * i + j] != NA_LOGICAL) {
                                element->set_logicalvalue(LOGICAL(mx)[rowLength * i + j]);
                            } else {
                                element->set_isnull(true);
                            }
                        }
                    } break;
                    case INTSXP: {
                        for (int j = 0; j < rowLength; j++) {
                            plcontainer::ScalarData *element =
                                result->mutable_rowvalues(j)->mutable_values(i);
                            if (INTEGER(mx)[rowLength * i + j] != NA_INTEGER) {
                                element->set_logicalvalue(
                                    (bool)INTEGER(mx)[rowLength * i + j] == 0 ? 0 : 1);
                            } else {
                                element->set_isnull(true);
                            }
                        }
                    } break;
                    case REALSXP: {
                        for (int j = 0; j < rowLength; j++) {
                            plcontainer::ScalarData *element =
                                result->mutable_rowvalues(j)->mutable_values(i);
                            if (REAL(mx)[rowLength * i + j] != NA_REAL) {
                                element->set_logicalvalue(
                                    (bool)REAL(mx)[rowLength * i + j] == 0 ? 0 : 1);
                            } else {
                                element->set_isnull(true);
                            }
                        }
                    } break;
                    default:
                        this->rLog->log(RServerLogLevel::ERRORS, "Unsupport type in value %d",
                                        subTypes[i]);
                        break;
                }
            } break;
            case plcontainer::PlcDataType::INT: {
                switch (TYPEOF(mx)) {
                    case LGLSXP: {
                        for (int j = 0; j < rowLength; j++) {
                            plcontainer::ScalarData *element =
                                result->mutable_rowvalues(j)->mutable_values(i);
                            if (LOGICAL(mx)[rowLength * i + j] != NA_LOGICAL) {
                                element->set_intvalue((int32_t)LOGICAL(mx)[rowLength * i + j]);
                            } else {
                                element->set_isnull(true);
                            }
                        }
                    } break;
                    case INTSXP: {
                        for (int j = 0; j < rowLength; j++) {
                            plcontainer::ScalarData *element =
                                result->mutable_rowvalues(j)->mutable_values(i);
                            if (INTEGER(mx)[rowLength * i + j] != NA_INTEGER) {
                                element->set_intvalue(INTEGER(mx)[rowLength * i + j]);
                            } else {
                                element->set_isnull(true);
                            }
                        }
                    } break;
                    case REALSXP: {
                        for (int j = 0; j < rowLength; j++) {
                            plcontainer::ScalarData *element =
                                result->mutable_rowvalues(j)->mutable_values(i);
                            if (REAL(mx)[rowLength * i + j] != NA_REAL) {
                                element->set_intvalue((int32_t)REAL(mx)[rowLength * i + j]);
                            } else {
                                element->set_isnull(true);
                            }
                        }
                    } break;
                    default:
                        this->rLog->log(RServerLogLevel::ERRORS, "Unsupport type in value %d",
                                        subTypes[i]);
                        break;
                }
            } break;
            case plcontainer::PlcDataType::REAL: {
                switch (TYPEOF(mx)) {
                    case LGLSXP: {
                        for (int j = 0; j < rowLength; j++) {
                            plcontainer::ScalarData *element =
                                result->mutable_rowvalues(j)->mutable_values(i);
                            if (LOGICAL(mx)[rowLength * i + j] != NA_LOGICAL) {
                                element->set_realvalue((double)LOGICAL(mx)[rowLength * i + j]);
                            } else {
                                element->set_isnull(true);
                            }
                        }
                    } break;
                    case INTSXP: {
                        for (int j = 0; j < rowLength; j++) {
                            plcontainer::ScalarData *element =
                                result->mutable_rowvalues(j)->mutable_values(i);
                            if (INTEGER(mx)[rowLength * i + j] != NA_INTEGER) {
                                element->set_realvalue((double)INTEGER(mx)[rowLength * i + j]);
                            } else {
                                element->set_isnull(true);
                            }
                        }
                    } break;
                    case REALSXP: {
                        for (int j = 0; j < rowLength; j++) {
                            plcontainer::ScalarData *element =
                                result->mutable_rowvalues(j)->mutable_values(i);
                            if (REAL(mx)[rowLength * i + j] != NA_REAL) {
                                element->set_realvalue(REAL(mx)[rowLength * i + j]);
                            } else {
                                element->set_isnull(true);
                            }
                        }
                    } break;
                    default:
                        this->rLog->log(RServerLogLevel::ERRORS, "Unsupport type in value %d",
                                        subTypes[i]);
                        break;
                }
            } break;
            case plcontainer::PlcDataType::TEXT: {
                switch (TYPEOF(mx)) {
                    case LGLSXP:
                    case INTSXP:
                    case REALSXP: {
                        SEXP strobj;
                        PROTECT(strobj = AS_CHARACTER(mx));
                        for (int j = 0; j < rowLength; j++) {
                            const char *value = CHAR(STRING_ELT(strobj, rowLength * i + j));
                            plcontainer::ScalarData *element =
                                result->mutable_rowvalues(j)->mutable_values(i);
                            if (STRING_ELT(strobj, rowLength * i + j) != NA_STRING &&
                                value != NULL) {
                                element->set_stringvalue(std::string(value));
                            } else {
                                element->set_isnull(true);
                            }
                        }
                        UNPROTECT_PTR(strobj);
                    } break;
                    case RAWSXP:
                    case STRSXP: {
                        for (int j = 0; j < rowLength; j++) {
                            const char *value = CHAR(STRING_ELT(mx, rowLength * i + j));
                            plcontainer::ScalarData *element =
                                result->mutable_rowvalues(j)->mutable_values(i);
                            if (STRING_ELT(mx, rowLength * i + j) != NA_STRING && value != NULL) {
                                element->set_stringvalue(std::string(value));
                            } else {
                                element->set_isnull(true);
                            }
                        }
                    } break;
                    default:
                        this->rLog->log(RServerLogLevel::ERRORS, "Unsupport type in value %d",
                                        subTypes[i]);
                        break;
                }
            } break;
            case plcontainer::PlcDataType::BYTEA: {
                switch (TYPEOF(mx)) {
                    case LGLSXP:
                    case INTSXP:
                    case REALSXP: {
                        SEXP strobj;
                        PROTECT(strobj = AS_CHARACTER(mx));
                        for (int j = 0; j < rowLength; j++) {
                            const char *value = CHAR(STRING_ELT(strobj, rowLength * i + j));
                            plcontainer::ScalarData *element =
                                result->mutable_rowvalues(j)->mutable_values(i);
                            if (STRING_ELT(strobj, rowLength * i + j) != NA_STRING &&
                                value != NULL) {
                                element->set_byteavalue(std::string(value));
                            } else {
                                element->set_isnull(true);
                            }
                        }
                        UNPROTECT_PTR(strobj);
                    } break;
                    case RAWSXP:
                    case STRSXP: {
                        for (int j = 0; j < rowLength; j++) {
                            const char *value = CHAR(STRING_ELT(mx, rowLength * i + j));
                            plcontainer::ScalarData *element =
                                result->mutable_rowvalues(j)->mutable_values(i);
                            if (STRING_ELT(mx, rowLength * i + j) != NA_STRING && value != NULL) {
                                element->set_byteavalue(std::string(value));
                            } else {
                                element->set_isnull(true);
                            }
                        }
                    } break;
                    default:
                        this->rLog->log(RServerLogLevel::ERRORS, "Unsupport type in value %d",
                                        subTypes[i]);
                        break;
                }

            } break;
            default:
                this->rLog->log(RServerLogLevel::ERRORS, "Unsupport type in value %d", subTypes[i]);
                break;
        }
    }
}

void ConvertToProtoBuf::setofToProtoBuf(SEXP v, std::vector<plcontainer::PlcDataType> &subTypes,
                                        plcontainer::SetOfData *result) {
    if (isFrame(v) || isList(v) || isNewList(v)) {
        // Both container factor
        this->dataframeToSetof(v, subTypes, result);
    } else if (isMatrix(v) || isVector(v)) {
        // No factor
        this->matrixToSetof(v, subTypes, result);
    } else {
        this->rLog->log(RServerLogLevel::ERRORS,
                        "Unsupport R object when return type is setof/record");
    }
}