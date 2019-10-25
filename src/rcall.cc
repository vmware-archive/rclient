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
        this->rLog.log(RServerLogLevel::WARNINGS, "R_HOME is not set, try use default R_HOME");
        r_home = (char *)std::string("/usr/lib/R").c_str();
    }

    rargc = sizeof(rargv) / sizeof(rargv[0]);

    // R_CStackLimit = (uintptr_t) -1;     /* disables R stack checking entirely
    // */

    if (!Rf_initEmbeddedR(rargc, (char **)rargv)) {
        // TODO: return an error
        this->rLog.log(RServerLogLevel::ERRORS, "can not start Embedded R");
    }

    /*
     * temporarily turn off R error reporting -- it will be turned back on
     * once the custom R error handler is installed from the plr library
     */

    if (this->loadRCmd(cmds[0].c_str()) < 0) {
        return ReturnStatus::FAIL;
    }

    if (this->loadRCmd(this->getLoadSelfRefCmd()) < 0) {
        return ReturnStatus::FAIL;
    }

    for (int i = 1; i < (int)cmds.size(); i++) {
        if (this->loadRCmd(cmds[i].c_str()) < 0) {
            return ReturnStatus::FAIL;
        }
    }

    R_CStackLimit = (uintptr_t) - 1; /* disables R stack checking entirely */

    return ReturnStatus::OK;
}

ReturnStatus RCoreRuntime::prepare(const CallRequest *callRequest) {
    this->rLog.log(RServerLogLevel::LOGS, "input length %ld, debug string is %s",
                   callRequest->ByteSizeLong(), callRequest->DebugString().c_str());

    if (this->prepareRFunctionSEXP(callRequest) != ReturnStatus::OK) {
        // TODO: error handling here
        return ReturnStatus::FAIL;
    }

    if (this->setArgumentValues(callRequest) != ReturnStatus::OK) {
        // TODO: error handling here
        return ReturnStatus::FAIL;
    }

    this->returnType = callRequest->rettype();
    return ReturnStatus::OK;
}

ReturnStatus RCoreRuntime::execute() {
    int errorOccurred;

    PROTECT(this->rFunc = lcons(this->rCode, this->rArgument));

    PrintValue(this->rFunc);

    PROTECT(this->rResults = R_tryEval(this->rFunc, R_GlobalEnv, &errorOccurred));

    if (this->rResults != nullptr) PrintValue(this->rResults);

    // free this->rFunc
    UNPROTECT_PTR(this->rFunc);
    UNPROTECT_PTR(this->rArgument);

    if (errorOccurred) {
        // TODO: error handling here
        return ReturnStatus::FAIL;
    } else {
        return ReturnStatus::OK;
    }
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

    if (this->parseRCode(rf) != ReturnStatus::OK) {
        // TODO: error handling here
        return ReturnStatus::FAIL;
    }

    return ReturnStatus::OK;
}

ReturnStatus RCoreRuntime::getResults(CallResponse *results) {
    ConvertToProtoBuf convert;
    PlcValue *ret = results->add_results();

    results->set_runtimetype(this->runType);
    ret->set_type(this->returnType);

    this->rLog.log(RServerLogLevel::LOGS, "return type is %d", this->returnType);

    switch (this->returnType) {
        case PlcDataType::LOGICAL: {
            ScalarData *data = ret->mutable_scalarvalue();
            data->set_type(PlcDataType::LOGICAL);
            data->set_logicalvalue(convert.boolToProtoBuf(this->rResults));
        } break;
        case PlcDataType::INT: {
            ScalarData *data = ret->mutable_scalarvalue();
            data->set_type(PlcDataType::INT);
            data->set_intvalue(convert.intToProtoBuf(this->rResults));
        } break;
        case PlcDataType::REAL: {
            ScalarData *data = ret->mutable_scalarvalue();
            data->set_type(PlcDataType::REAL);
            data->set_realvalue(convert.realToProtoBuf(this->rResults));
        } break;
        case PlcDataType::TEXT: {
            ScalarData *data = ret->mutable_scalarvalue();
            data->set_type(PlcDataType::TEXT);
            data->set_stringvalue(convert.textToProtoBuf(this->rResults));
        } break;
        case PlcDataType::BYTEA: {
            ScalarData *data = ret->mutable_scalarvalue();
            data->set_type(PlcDataType::BYTEA);
            data->set_byteavalue(convert.byteaToProtoBuf(this->rResults));
        } break;
        default:
            return ReturnStatus::FAIL;
    }

    return ReturnStatus::OK;
}

void RCoreRuntime::cleanup() {
    UNPROTECT_PTR(this->rResults);
    UNPROTECT_PTR(this->rCode);
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
        this->rLog.log(RServerLogLevel::ERRORS, "can not read execute path");
    } else {
        path[size] = '\0';
    }

    this->rLog.log(RServerLogLevel::LOGS, "Current R client path is %s", path);
    p = strrchr(path, '/');
    if (p) {
        *(p + 1) = '\0';
    } else {
        this->rLog.log(RServerLogLevel::ERRORS, "can not read execute directory %s", path);
    }

    this->rLog.log(RServerLogLevel::LOGS, "Split path by '/'. Get the path: %s", path);
    buf = "dyn.load(\"" + std::string(path) + "/librcall.so\")";
#else
    buf = "dyn.load(\"librcall.so\")";
#endif
    return buf;
}

// TODO: private function, may use Rcpp instead of
int RCoreRuntime::loadRCmd(const std::string &cmd) {
    SEXP cmdSexp, cmdexpr;
    int i, status = 0;

    try {
        PROTECT(cmdSexp = NEW_CHARACTER(1));
        SET_STRING_ELT(cmdSexp, 0, COPY_TO_USER_STRING(cmd.c_str()));
        PROTECT(cmdexpr = R_PARSEVECTOR(cmdSexp, -1, &status));
        if (status != PARSE_OK) {
            throw rParseErrorException(cmd);
        }

        /* Loop is needed here as EXPSEXP may be of length > 1 */
        for (i = 0; i < Rf_length(cmdexpr); i++) {
            R_tryEval(VECTOR_ELT(cmdexpr, i), R_GlobalEnv, &status);
            if (status != 0) {
                throw rParseErrorException(cmd);
            }
        }

        UNPROTECT_PTR(cmdSexp);
        UNPROTECT_PTR(cmdexpr);
        return 0;
    }
    catch (std::exception &e) {
        UNPROTECT_PTR(cmdSexp);
        UNPROTECT_PTR(cmdexpr);
        this->rLog.log(RServerLogLevel::WARNINGS, "R server catch an expection: %s", e.what());
        return -1;
    }
}

ReturnStatus RCoreRuntime::parseRCode(const std::string &code) {
    ParseStatus status;
    ReturnStatus rs;
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
        rs = ReturnStatus::FAIL;
    } else {
        rs = ReturnStatus::OK;
    }

    UNPROTECT_PTR(rbody);
    UNPROTECT_PTR(tmp);
    return rs;
}

std::string RCoreRuntime::getRsourceCode(const userFunction &userCode) {
    std::string rFuncSource;

    rFuncSource = "gpdbR." + userCode.getFunctionName() + " <- function(";

    this->rLog.log(RServerLogLevel::LOGS, "the number of argment is %d",
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
    ConvertToSEXP convter;
    int count = 0;

    /* create the argument list */
    PROTECT(this->rArgument = allargs = allocList(callRequest->args_size()));

    try {
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
                    rArg =
                        convter.boolToSEXP(callRequest->args()[count].scalarvalue().logicalvalue());
                    break;
                }
                case PlcDataType::INT: {
                    rArg = convter.intToSEXP(callRequest->args()[count].scalarvalue().intvalue());
                    break;
                }
                case PlcDataType::REAL: {
                    rArg = convter.realToSEXP(callRequest->args()[count].scalarvalue().realvalue());
                    break;
                }
                case PlcDataType::TEXT: {
                    rArg =
                        convter.textToSEXP(callRequest->args()[count].scalarvalue().stringvalue());
                    break;
                }
                case PlcDataType::BYTEA: {
                    rArg =
                        convter.byteaToSEXP(callRequest->args()[count].scalarvalue().byteavalue());
                    break;
                }
                default:
                    throw rArgumentErrorException("Unsupport type in args " +
                                                  callRequest->args()[count].name());
                    break;
            }

            SETCAR(allargs, rArg);
            allargs = CDR(allargs);
        }
    }
    catch (std::exception &e) {
        /* we've made it to the i'th argument */
        UNPROTECT(count + 1);
        this->rLog.log(RServerLogLevel::WARNINGS, "R server receive an error %s", e.what());
        return ReturnStatus::FAIL;
    }

    /* this->rArgument need to be protected */
    UNPROTECT(count);
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
