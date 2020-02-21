/*------------------------------------------------------------------------------
 *
 * Copyright (c) 2016-Present Pivotal Software, Inc
 *
 *------------------------------------------------------------------------------
 */
/* Global header files */
#include <cstdio>
#include <string>
#include <vector>

/* System header files */
#include <unistd.h>

/* PL/Container header files */
#include "rcall.hh"

#define CSTACK_DEFNS 1
/* Extra R headers */
#include <R_ext/Parse.h>
#include <Rinterface.h>

using namespace plcontainer;

/* Extra R settings */
#define ERR_MSG_LENGTH 512

#if (R_VERSION >= 132352) /* R_VERSION >= 2.5.0 */
#define R_PARSEVECTOR(a_, b_, c_) R_ParseVector(a_, b_, (ParseStatus *)c_, R_NilValue)
#else /* R_VERSION < 2.5.0 */
#define R_PARSEVECTOR(a_, b_, c_) R_ParseVector(a_, b_, (ParseStatus *)c_)
#endif /* R_VERSION >= 2.5.0 */

#ifndef PATH_MAX
#define PATH_MAX 2048
#endif

#define TYPE_ID_LENGTH 12

#define OPTIONS_NULL_CMD "options(error = expression(NULL))"

/* install the error handler to call our throw_r_error */
#define THROWRERROR_CMD                                 \
    "pg.throwrerror <-function(msg) "                   \
    "{"                                                 \
    "  msglen <- nchar(msg);"                           \
    "  if (substr(msg, msglen, msglen + 1) == \"\\n\")" \
    "    msg <- substr(msg, 1, msglen - 1);"            \
    "  .C(\"throw_r_error\", as.character(msg));"       \
    "}"
#define OPTIONS_THROWRERROR_CMD "options(error = expression(pg.throwrerror(geterrmessage())))"

/* install the notice handler to call our throw_r_notice */
#define THROWNOTICE_CMD               \
    "pg.thrownotice <-function(msg) " \
    "{.C(\"throw_pg_notice\", as.character(msg))}"
#define THROWERROR_CMD               \
    "pg.throwerror <-function(msg) " \
    "{stop(msg, call. = FALSE)}"
#define OPTIONS_THROWWARN_CMD \
    "options(warning.expression = expression(pg.thrownotice(last.warning)))"

/* R interface */
extern "C" {
void throw_pg_notice(const char **msg);

void throw_r_error(const char **msg);
}

/* Globals */

/* Exposed in R_interface.h */
int R_SignalHandlers = 1;

/* set by hook throw_r_error */
static char *last_R_error_msg, *last_R_notice;

ReturnStatus RCoreRuntime::init() {
    const char *rargv[] = {"rclient", "--slave", "--silent", "--no-save", "--no-restore"};
    char *r_home;
    int rargc;
    std::vector<std::string> cmds = {
        /* first turn off error handling by R */
        OPTIONS_NULL_CMD,

        /* set up the postgres error handler in R */
        THROWRERROR_CMD,  OPTIONS_THROWRERROR_CMD, THROWNOTICE_CMD,
        THROWERROR_CMD,   OPTIONS_THROWWARN_CMD,

        /* terminate */
    };

    r_home = getenv("R_HOME");
    /*
* Stop R using its own signal handlers Otherwise, R will prompt the user for
what to do and will hang in the container
*/
    R_SignalHandlers = 0;
    if (r_home == NULL) {
        /* TODO: fix if R_HOME is not set (i.e., defalut vaule) */
        this->rLog->log(RServerLogLevel::WARNINGS, "R_HOME is not set, try use default R_HOME");
        r_home = (char *)std::string("/usr/lib/R").c_str();
    }

    rargc = sizeof(rargv) / sizeof(rargv[0]);

    if (!Rf_initEmbeddedR(rargc, (char **)rargv)) {
        // TODO: return an error
        this->rLog->log(RServerLogLevel::FATALS, "can not start Embedded R");
    }

    /*
     * temporarily turn off R error reporting -- it will be turned back on
     * once the custom R error handler is installed from the plr library
     */

    this->loadRCmd(cmds[0].c_str());

    this->loadRCmd(this->getLoadSelfRefCmd());

    for (int i = 1; i < (int)cmds.size(); i++) {
        this->loadRCmd(cmds[i].c_str());
    }

    R_CStackLimit = (uintptr_t) - 1; /* disables R stack checking entirely */

    // reset the vaule of counter
    this->counter = 0;

    return ReturnStatus::OK;
}

ReturnStatus RCoreRuntime::prepare(const CallRequest *callRequest) {
    this->rLog->log(RServerLogLevel::LOGS, "input length %ld, debug string is %s",
                    callRequest->ByteSizeLong(), callRequest->DebugString().c_str());

    if (this->prepareRFunctionSEXP(callRequest) != ReturnStatus::OK) {
        // TODO: error handling here
        return ReturnStatus::FAIL;
    }

    this->setArgumentValues(callRequest);

    this->returnType = callRequest->rettype().type();

    if (callRequest->rettype().type() == PlcDataType::COMPOSITE ||
        callRequest->rettype().type() == PlcDataType::ARRAY ||
        callRequest->rettype().type() == PlcDataType::SETOF) {
        // We also need to copy subtypes of UDT/Array as this stage
        for (int i = 0; i < callRequest->rettype().subtypes_size(); i++) {
            this->returnSubType.emplace_back(callRequest->rettype().subtypes(i));
        }
    }

    return ReturnStatus::OK;
}

ReturnStatus RCoreRuntime::execute() {
    int errorOccurred;

    PROTECT(this->rFunc = lcons(this->rCode, this->rArgument));

    PROTECT(this->rResults = R_tryEval(this->rFunc, R_GlobalEnv, &errorOccurred));

    if (this->rResults != nullptr) PrintValue(this->rResults);

    // free this->rFunc
    UNPROTECT_PTR(this->rFunc);
    this->rFunc = nullptr;

    UNPROTECT_PTR(this->rArgument);
    this->rArgument = nullptr;

    if (errorOccurred) {
        this->rLog->log(RServerLogLevel::ERRORS, "Unable execute user code");
    }

    return ReturnStatus::OK;
}

ReturnStatus RCoreRuntime::prepareRFunctionSEXP(const CallRequest *callRequest) {
    struct userFunction uf;
    std::string rf;

    // build R function
    uf.setFunctionName(callRequest->proc().name());
    uf.setFunctionBody(callRequest->proc().src());

    for (int i = 0; i < callRequest->args_size(); i++) {
        uf.addFunctionArgsName(callRequest->args()[i].name());
    }

    rf = this->getRsourceCode(uf);

    this->parseRCode(rf);

    return ReturnStatus::OK;
}

ReturnStatus RCoreRuntime::getResults(CallResponse *results) {
    ConvertToProtoBuf *convert = new ConvertToProtoBuf(this->rLog);
    PlcValue *ret = results->add_results();

    results->set_runtimetype(this->runType);
    ret->set_type(this->returnType);

    this->rLog->log(RServerLogLevel::LOGS, "return type is %d", this->returnType);

    switch (this->returnType) {
        case PlcDataType::LOGICAL: {
            ScalarData *data = ret->mutable_scalarvalue();
            data->set_type(PlcDataType::LOGICAL);
            convert->scalarToProtobuf(this->rResults, this->returnType, data);
        } break;
        case PlcDataType::INT: {
            ScalarData *data = ret->mutable_scalarvalue();
            data->set_type(PlcDataType::INT);
            convert->scalarToProtobuf(this->rResults, this->returnType, data);
        } break;
        case PlcDataType::REAL: {
            ScalarData *data = ret->mutable_scalarvalue();
            data->set_type(PlcDataType::REAL);
            convert->scalarToProtobuf(this->rResults, this->returnType, data);
        } break;
        case PlcDataType::TEXT: {
            ScalarData *data = ret->mutable_scalarvalue();
            data->set_type(PlcDataType::TEXT);
            convert->scalarToProtobuf(this->rResults, this->returnType, data);
        } break;
        case PlcDataType::BYTEA: {
            ScalarData *data = ret->mutable_scalarvalue();
            data->set_type(PlcDataType::BYTEA);
            convert->scalarToProtobuf(this->rResults, this->returnType, data);
        } break;
        case PlcDataType::COMPOSITE: {
            CompositeData *cdata = ret->mutable_compositevalue();
            convert->compositeToProtoBuf(this->rResults, this->returnSubType, cdata);
        } break;
        case PlcDataType::ARRAY: {
            ArrayData *adata = ret->mutable_arrayvalue();
            // Currently, we only support 1-D array, so use index=0
            convert->arrayToProtoBuf(this->rResults, this->returnSubType[0], adata);
        } break;
        case PlcDataType::SETOF: {
            SetOfData *sdata = ret->mutable_setofvalue();
            convert->setofToProtoBuf(this->rResults, this->returnSubType, sdata);
        } break;
        case PlcDataType::VOID: {
            // No return needed
        } break;
        default:
            delete convert;
            this->rLog->log(RServerLogLevel::ERRORS, "Unsupport return type %d", this->returnType);
            break;
    }

    delete convert;
    return ReturnStatus::OK;
}

void RCoreRuntime::cleanup() {
    this->rLog->log(RServerLogLevel::LOGS, "try free result sexp point");
    if (this->rResults != nullptr) {
        UNPROTECT_PTR(this->rResults);
        this->rResults = nullptr;
    }
    this->rLog->log(RServerLogLevel::LOGS, "try free r code sexp point");
    if (this->rCode != nullptr) {
        UNPROTECT_PTR(this->rCode);
        this->rCode = nullptr;
    }
    // also clear the subtype vector
    this->returnSubType.clear();

    // increase the counter
    this->counter++;
    this->rLog->log(RServerLogLevel::LOGS, "finish the %d calls", this->counter);
}

std::string RCoreRuntime::getLoadSelfRefCmd() {
    std::string buf;

#ifdef __linux__
    char path[PATH_MAX];
    char *p = NULL;
    int size;
    /* next load the plr library into R */
    size = readlink("/proc/self/exe", path, PATH_MAX);
    if (size == -1) {
        this->rLog->log(RServerLogLevel::FATALS, "can not read execute path");
    } else {
        path[size] = '\0';
    }

    this->rLog->log(RServerLogLevel::LOGS, "Current R server path is %s", path);
    p = strrchr(path, '/');
    if (p) {
        *(p + 1) = '\0';
    } else {
        this->rLog->log(RServerLogLevel::FATALS, "can not read execute directory %s", path);
    }

    this->rLog->log(RServerLogLevel::LOGS, "Split path by '/'. Get the path: %s", path);
    buf = "dyn.load(\"" + std::string(path) + "/librcall.so\")";
#else
    buf = "dyn.load(\"librcall.so\")";
#endif
    return buf;
}

// TODO: private function, may use Rcpp instead of
void RCoreRuntime::loadRCmd(const std::string &cmd) {
    SEXP cmdSexp = NULL;
    SEXP cmdexpr = NULL;
    int i, status = 0;

    try {
        PROTECT(cmdSexp = NEW_CHARACTER(1));
        SET_STRING_ELT(cmdSexp, 0, COPY_TO_USER_STRING(cmd.c_str()));
        PROTECT(cmdexpr = R_PARSEVECTOR(cmdSexp, -1, &status));
        if (status != PARSE_OK) {
            this->rLog->log(RServerLogLevel::FATALS, "Cannot process R cmd %s", cmd.c_str());
        }

        /* Loop is needed here as EXPSEXP may be of length > 1 */
        for (i = 0; i < Rf_length(cmdexpr); i++) {
            R_tryEval(VECTOR_ELT(cmdexpr, i), R_GlobalEnv, &status);
            if (status != 0) {
                this->rLog->log(RServerLogLevel::FATALS, "Cannot process R cmd %s", cmd.c_str());
            }
        }
        UNPROTECT_PTR(cmdSexp);
        UNPROTECT_PTR(cmdexpr);
    }
    catch (std::exception &e) {
        this->rLog->log(RServerLogLevel::LOGS, "try free exception init sexp point");
        UNPROTECT_PTR(cmdSexp);
        UNPROTECT_PTR(cmdexpr);
        throw e;
    }
}

ReturnStatus RCoreRuntime::parseRCode(const std::string &code) {
    ParseStatus status;
    SEXP tmp, rbody;

    PROTECT(rbody = mkString(code.c_str()));
    /*
limit the number of expressions to be parsed to 2:
- the definition of the function, i.e. f <- function() {...}
- the call to the function f()

kind of useful to prevent injection, but pointless since we are
running in a container. I think -1 is equivalent to no limit.
*/
    PROTECT(tmp = R_PARSEVECTOR(rbody, -1, &status));

    if (tmp != R_NilValue) {
        PROTECT(this->rCode = VECTOR_ELT(tmp, 0));
    } else {
        PROTECT(this->rCode = R_NilValue);
    }

    if (status != PARSE_OK) {
        this->rLog->log(RServerLogLevel::ERRORS, "Cannot parse user code %s", code.c_str());
    }
    UNPROTECT_PTR(rbody);
    UNPROTECT_PTR(tmp);
    return ReturnStatus::OK;
}

std::string RCoreRuntime::getRsourceCode(const userFunction &userCode) {
    std::string rFuncSource;

    rFuncSource = "gpdbR." + userCode.getFunctionName() + " <- function(";

    this->rLog->log(RServerLogLevel::LOGS, "the number of argment is %d",
                    userCode.getFunctionArgsName().size());
    if (userCode.getFunctionArgsName().size() != 0) {
        rFuncSource = rFuncSource + userCode.getFunctionArgsName()[0];
    }

    for (int i = 1; i < (int)userCode.getFunctionArgsName().size(); i++) {
        rFuncSource = rFuncSource + ", " + userCode.getFunctionArgsName()[i];
    }

    rFuncSource = rFuncSource + ") {" + userCode.getFunctionBody() + "}";

    return rFuncSource;
}

ReturnStatus RCoreRuntime::setArgumentValues(const CallRequest *callRequest) {
    SEXP allargs, rArg;
    ConvertToSEXP *convert = new ConvertToSEXP(this->rLog);
    int count = 0;

    /* create the argument list */
    PROTECT(this->rArgument = allargs = allocList(callRequest->args_size()));

    for (count = 0; count < callRequest->args_size(); count++) {
        PlcDataType type;

        if (callRequest->args()[count].scalarvalue().isnull()) {
            PROTECT(rArg = R_NilValue);
            continue;
        } else {
            type = callRequest->args()[count].type();
        }

        switch (type) {
            case PlcDataType::LOGICAL: {
                rArg = convert->boolToSEXP(callRequest->args()[count].scalarvalue().logicalvalue());
                break;
            }
            case PlcDataType::INT: {
                rArg = convert->intToSEXP(callRequest->args()[count].scalarvalue().intvalue());
                break;
            }
            case PlcDataType::REAL: {
                rArg = convert->realToSEXP(callRequest->args()[count].scalarvalue().realvalue());
                break;
            }
            case PlcDataType::TEXT: {
                rArg = convert->textToSEXP(callRequest->args()[count].scalarvalue().stringvalue());
                break;
            }
            case PlcDataType::BYTEA: {
                rArg = convert->byteaToSEXP(callRequest->args()[count].scalarvalue().byteavalue());
                break;
            }
            case PlcDataType::COMPOSITE: {
                rArg = convert->compositeToSEXP(callRequest->args()[count].compositevalue());
                break;
            }
            case PlcDataType::ARRAY: {
                rArg = convert->arrayToSEXP(callRequest->args()[count].arrayvalue());
                break;
            }
            case PlcDataType::SETOF: {
                rArg = convert->setofToSEXP(callRequest->args()[count].setofvalue());
                break;
            }
            default:
                UNPROTECT(count + 1);
                delete convert;
                this->rLog->log(RServerLogLevel::ERRORS, "Unsupport type in args %s",
                                callRequest->args()[count].name().c_str());
                return ReturnStatus::FAIL;
        }

        SETCAR(allargs, rArg);
        allargs = CDR(allargs);
    }
    /* this->rArgument need to be protected */
    UNPROTECT(count);
    delete convert;
    return ReturnStatus::OK;
}

void throw_pg_notice(const char **msg) {
    if (msg && *msg) last_R_notice = strdup(*msg);
}

void throw_r_error(const char **msg) {
    if (msg && *msg)
        last_R_error_msg = strdup(*msg);
    else
        last_R_error_msg = strdup("caught error calling R function");
}

#ifdef DEBUGPROTECT
int balance = 0;
SEXP pg_protect(SEXP s, char *fn, int ln) {
    balance++;
    plc_elog(NOTICE, "%d\tPROTECT\t1\t%s\t%d", balance, fn, ln);
    return protect(s);
}

void pg_unprotect(int n, char *fn, int ln) {
    balance = balance - n;
    plc_elog(NOTICE, "%d\tUNPROTECT\t%d\t%s\t%d", balance, n, fn, ln);
    unprotect(n);
}
#endif /* DEBUGPROTECT */
