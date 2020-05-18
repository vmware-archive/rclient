#ifndef _R_SERVER_H
#define _R_SERVER_H

#include <mutex>

#include <grpc/grpc.h>
#include <grpc/status.h>
#include <grpcpp/security/server_credentials.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>

#include "rcall.hh"
#include "rutils.hh"

#include "plcontainer.grpc.pb.h"
#include "plcontainer.pb.h"

using namespace plcontainer;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReader;
using grpc::ServerReaderWriter;
using grpc::ServerWriter;
using grpc::Status;

#define UDS_SHARED_ADDRESS "unix:///tmp/plcontainer/unix.domain.socket.shared.file"
#define UDS_SHARED_FILE "/tmp/plcontainer/unix.domain.socket.shared.file"

#define IPC_CLIENT_DIR "/tmp/plcontainer"
class RServerRPC final : public PLContainer::Service {
   public:
    RServerRPC(RServerWorkingMode mode, RServerLog *rLog) {
        this->runtime = nullptr;
        this->rLog = rLog;
        this->serverWorkingMode = mode;
        // this->numberOfSession = 0; reserved for next step
    }

    virtual void initRCore();
    void setLogger(RServerLog *log) { this->rLog = log; }

    virtual Status FunctionCall(ServerContext *context, const CallRequest *callRequest,
                                CallResponse *result) override;

    virtual ~RServerRPC() {
        if (this->runtime != nullptr) {
            delete this->runtime;
        }
    }

   private:
    Status singleSessionRuntime(ServerContext *context, const CallRequest *callRequest,
                                CallResponse *result);
    Status multiSessionRuntime(ServerContext *context, const CallRequest *callRequest,
                               CallResponse *result);
    PlcRuntime *runtime;
    RServerLog *rLog;
    std::mutex mtx;
    // int64_t numberOfSession; reserved for next step
    RServerWorkingMode serverWorkingMode;
};

class RServer {
   public:
    RServer(RServerWorkingMode mode, RServerLog *rLog) {
        this->serverWorkingMode = mode;
        this->rLog = rLog;
        server = new RServerRPC(mode, rLog);
    }

    virtual int initServer(const std::string &address, const std::string &port);
    virtual int initServer(const std::string &udsFile);
    virtual int initServer();

    virtual int startServer();

    void setLogger(RServerLog *log) { this->rLog = log; }

    virtual ~RServer() { delete server; };

   private:
    RServerWorkingMode serverWorkingMode;
    std::string serverAddress;
    RServerRPC *server;
    RServerLog *rLog;

    void udsCheck(const std::string &uds);
};

#endif  //_R_SERVER_H
