/*------------------------------------------------------------------------------
 *
 * Copyright (c) 2016-Present Pivotal Software, Inc
 *
 *------------------------------------------------------------------------------
 */
#include <cstdlib>
#include <cassert>
#include <climits>
#include <cerrno>
#include <cstring>
#include <exception>

#include <unistd.h>
#include <sys/stat.h>

#include "rserver.hh"

int RServer::startServer() {
    ServerBuilder builder;

    builder.AddListeningPort(this->serverAddress, grpc::InsecureServerCredentials());
    builder.RegisterService(this->server);
    std::unique_ptr<Server> server(builder.BuildAndStart());

    if (this->serverWorkingMode == RServerWorkingMode::CONTAINER ||
        this->serverWorkingMode == RServerWorkingMode::CONTAINERDEBUG) {
        this->rLog->log(RServerLogLevel::LOGS, "Container mode, check uds permission");
        this->udsCheck(UDS_SHARED_FILE);
    }

    this->rLog->log(RServerLogLevel::LOGS, "Server is waiting for query");
    server->Wait();

    return 0;
}

int RServer::initServer(const std::string &address, const std::string &port) {
    this->serverAddress = address + ":" + port;
    this->server->initRCore();
    return 0;
}

int RServer::initServer(const std::string &udsFile) {
    this->serverAddress = "unix://" + udsFile;
    this->server->initRCore();

    return 0;
}

int RServer::initServer() {
    this->serverAddress = UDS_SHARED_ADDRESS;
    this->server->initRCore();

    return 0;
}

void RServer::udsCheck(const std::string &uds) {
    char *env_str, *endptr;
    uid_t qe_uid;
    gid_t qe_gid;
    long val;

    // The path owner should be generally the uid, but we are not 100% sure
    // about this for current/future backends, so we still use environment
    // variable, instead of extracting them via reading the owner of the path.

    // Get executor uid: for permission of the unix domain socket file.

    if ((env_str = getenv("EXECUTOR_UID")) == NULL)
        this->rLog->log(RServerLogLevel::FATALS,
                        "EXECUTOR_UID is not set, something wrong on QE side");
    errno = 0;
    val = strtol(env_str, &endptr, 10);
    if ((errno == ERANGE && (val == LONG_MAX || val == LONG_MIN)) || (errno != 0 && val == 0) ||
        endptr == env_str || *endptr != '\0') {
        this->rLog->log(RServerLogLevel::FATALS, "EXECUTOR_UID is wrong:'%s'", env_str);
    }
    qe_uid = val;

    // Get executor gid: for permission of the unix domain socket file.
    if ((env_str = getenv("EXECUTOR_GID")) == NULL)
        this->rLog->log(RServerLogLevel::FATALS,
                        "EXECUTOR_GID is not set, something wrong on QE side");
    errno = 0;
    val = strtol(env_str, &endptr, 10);
    if ((errno == ERANGE && (val == LONG_MAX || val == LONG_MIN)) || (errno != 0 && val == 0) ||
        endptr == env_str || *endptr != '\0') {
        this->rLog->log(RServerLogLevel::FATALS, "EXECUTOR_GID is wrong:'%s'", env_str);
    }
    qe_gid = val;

    // Change ownership & permission for the file for unix domain socket so
    // code on the QE side could access it and clean up it later.

    if (chown(uds.c_str(), qe_uid, qe_gid) < 0)
        this->rLog->log(RServerLogLevel::FATALS,
                        "Could not set ownership for file %s with owner %d, "
                        "group %d: %s",
                        uds.c_str(), qe_uid, qe_gid, strerror(errno));
    if (chmod(uds.c_str(), S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH) < 0) /* 0666*/
        this->rLog->log(RServerLogLevel::FATALS, "Could not set permission for file %s: %s",
                        uds.c_str(), strerror(errno));
}

void RServerRPC::initRCore() {
    this->runtime = new RCoreRuntime(this->rLog);
    this->rLog->log(RServerLogLevel::LOGS, "start to init RCore");
    ReturnStatus status = this->runtime->init();

    if (status != ReturnStatus::OK) {
        this->rLog->log(RServerLogLevel::FATALS, "Cannot init R core");
    }

    this->rLog->log(RServerLogLevel::LOGS, "RCore init success");
}

Status RServerRPC::singleSessionRuntime(ServerContext *context, const CallRequest *callRequest,
                                        CallResponse *result) {
    try {
        if (!this->mtx.try_lock()) {
            this->rLog->log(RServerLogLevel::ERRORS,
                            "The previous query is still running, cannot accpet new query. Maybe "
                            "client is timeout");
        }
        this->rLog->log(RServerLogLevel::LOGS, "start to process query");
        this->runtime->prepare(callRequest);
        this->runtime->execute();

        // If cancelled, we do not need to process the results
        if (context->IsCancelled()) {
            this->rLog->log(RServerLogLevel::WARNINGS, "this query has been cancelled by client");
            this->runtime->cleanup();
            this->mtx.unlock();
            return Status::CANCELLED;
        }

        this->runtime->getResults(result);
        this->runtime->cleanup();
        result->set_logs(this->rLog->getLogBuffer());
        this->rLog->resetLogBuffer();
        this->mtx.unlock();
    }
    catch (RServerFatalException &e) {
        Error *err = result->mutable_exception();
        Status fatalStatus = Status(grpc::StatusCode::INTERNAL, grpc::string(e.what()));
        err->set_message(e.what());
        result->set_logs(this->rLog->getLogBuffer());
        this->rLog->resetLogBuffer();
        this->mtx.unlock();

        return fatalStatus;

        // TODO: clear up all/cached SEXP content
    }
    catch (RServerErrorException &e) {
        Error *err = result->mutable_exception();
        Status errorStatus = Status(grpc::StatusCode::FAILED_PRECONDITION, grpc::string(e.what()));
        err->set_message(e.what());
        result->set_logs(this->rLog->getLogBuffer());
        this->rLog->resetLogBuffer();
        this->mtx.unlock();

        return errorStatus;
    }
    return Status::OK;
}

Status RServerRPC::multiSessionRuntime(ServerContext *context, const CallRequest *callRequest,
                                       CallResponse *result) {
    RServerLog *log = new RServerLog(this->serverWorkingMode, std::string(""));
    RCoreRuntime *runtime = new RCoreRuntime(log);

    try {
        log->log(RServerLogLevel::LOGS, "start to a new session");
        runtime->prepare(callRequest);
        log->log(RServerLogLevel::LOGS, "start to process query");

        runtime->execute();

        // If cancelled, we do not need to process the results
        if (context->IsCancelled()) {
            log->log(RServerLogLevel::WARNINGS, "this query has been cancelled by client");
            runtime->cleanup();
            return Status::CANCELLED;
        }

        runtime->getResults(result);
        runtime->cleanup();
        result->set_logs(log->getLogBuffer());
        log->resetLogBuffer();
    }
    catch (RServerFatalException &e) {
        Error *err = result->mutable_exception();
        Status fatalStatus = Status(grpc::StatusCode::INTERNAL, grpc::string(e.what()));
        err->set_message(e.what());
        result->set_logs(log->getLogBuffer());
        log->resetLogBuffer();
        runtime->cleanup();

        delete runtime;
        delete log;

        return fatalStatus;

        // TODO: clear up all/cached SEXP content
    }
    catch (RServerErrorException &e) {
        Error *err = result->mutable_exception();
        Status errorStatus = Status(grpc::StatusCode::FAILED_PRECONDITION, grpc::string(e.what()));
        err->set_message(e.what());
        result->set_logs(this->rLog->getLogBuffer());
        log->resetLogBuffer();
        runtime->cleanup();

        delete runtime;
        delete log;

        return errorStatus;
    }

    // clean the R runtime session
    delete runtime;
    delete log;

    return Status::OK;
}

Status RServerRPC::FunctionCall(ServerContext *context, const CallRequest *callRequest,
                                CallResponse *result) {
    switch (this->serverWorkingMode) {
        // Currently only PL4K related mode need multiple session support
        case RServerWorkingMode::PL4K:
        case RServerWorkingMode::PL4KDEBUG:
            return this->multiSessionRuntime(context, callRequest, result);
        default:
            return this->singleSessionRuntime(context, callRequest, result);
    }
}
