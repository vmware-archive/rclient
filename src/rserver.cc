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

#include <unistd.h>
#include <sys/stat.h>

#include "rserver.hh"

int RServer::startServer()
{
	ServerBuilder builder;

	builder.AddListeningPort(this->udsAddress, grpc::InsecureServerCredentials());
	builder.RegisterService(&this->server);
	std::unique_ptr<Server> server(builder.BuildAndStart());
	this->rLog.log(RServerLogLevel::LOGS, "Server is waiting for query");
	server->Wait();

	return 0;
}

int RServer::initServer(const std::string &udsFile)
{
	this->udsAddress = "unix://" + udsFile;
	this->server.initRCore();

	return 0;
}

int RServer::initServer()
{
	udsCheck(UDS_SHARED_FILE);
	this->udsAddress = UDS_SHARED_ADDRESS;
	this->server.initRCore();

	return 0;
}

void RServer::udsCheck(const std::string &uds)
{
	char *env_str, *endptr;
	uid_t qe_uid;
	gid_t qe_gid;
	long val;

	/*
	 * The path owner should be generally the uid, but we are not 100% sure
	 * about this for current/future backends, so we still use environment
	 * variable, instead of extracting them via reading the owner of the path.
	 */

	/* Get executor uid: for permission of the unix domain socket file. */

	if ((env_str = getenv("EXECUTOR_UID")) == NULL)
		this->rLog.log(RServerLogLevel::ERRORS, "EXECUTOR_UID is not set, something wrong on QE side");
	errno = 0;
	val = strtol(env_str, &endptr, 10);
	if ((errno == ERANGE && (val == LONG_MAX || val == LONG_MIN)) ||
		(errno != 0 && val == 0) ||
		endptr == env_str ||
		*endptr != '\0')
	{
		this->rLog.log(RServerLogLevel::ERRORS, "EXECUTOR_UID is wrong:'%s'", env_str);
	}
	qe_uid = val;

	/* Get executor gid: for permission of the unix domain socket file. */
	if ((env_str = getenv("EXECUTOR_GID")) == NULL)
		this->rLog.log(RServerLogLevel::ERRORS, "EXECUTOR_GID is not set, something wrong on QE side");
	errno = 0;
	val = strtol(env_str, &endptr, 10);
	if ((errno == ERANGE && (val == LONG_MAX || val == LONG_MIN)) ||
		(errno != 0 && val == 0) ||
		endptr == env_str ||
		*endptr != '\0')
	{
		this->rLog.log(RServerLogLevel::ERRORS, "EXECUTOR_GID is wrong:'%s'", env_str);
	}
	qe_gid = val;

	/* Change ownership & permission for the file for unix domain socket so
	 * code on the QE side could access it and clean up it later.
	 */
	if (chown(uds.c_str(), qe_uid, qe_gid) < 0)
		this->rLog.log(RServerLogLevel::ERRORS, "Could not set ownership for file %s with owner %d, "
												"group %d: %s",
					   uds.c_str(), qe_uid, qe_gid, strerror(errno));
	if (chmod(uds.c_str(), S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH) < 0) /* 0666*/
		this->rLog.log(RServerLogLevel::ERRORS, "Could not set permission for file %s: %s",
					   uds.c_str(), strerror(errno));
}

void RServerRPC::initRCore()
{
	this->runtime = new RCoreRuntime();
	this->rLog.log(RServerLogLevel::LOGS, "start to init RCore");
	ReturnStatus status = this->runtime->init();
	this->rLog.log(RServerLogLevel::LOGS, "RCore inited");

	if (status != ReturnStatus::OK)
	{
		this->rLog.log(RServerLogLevel::ERRORS, "Cannot init R core");
	}
}

Status RServerRPC::FunctionCall(ServerContext *context, const CallRequest *callRequest, CallResponse *result)
{
	(void)context;

	this->runtime->prepare(callRequest);
	this->runtime->execute();
	this->runtime->getResults(result);
	this->runtime->cleanup();

	return Status::OK;
}
