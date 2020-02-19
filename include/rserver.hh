#ifndef _R_SERVER_H
#define _R_SERVER_H

#include <mutex>

#include <grpc/grpc.h>
#include <grpc/status.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>
#include <grpcpp/security/server_credentials.h>

#include "rutils.hh"
#include "rcall.hh"

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
    RServerRPC(RServerLog *rLog) {
        this->runtime = nullptr;
        this->rLog = rLog;
    }

    virtual void initRCore();
    void setLogger(RServerLog *log) { this->rLog = log; }

    virtual Status FunctionCall(ServerContext *context, const CallRequest *callRequest,
                                CallResponse *result) override;

    virtual ~RServerRPC() { delete this->runtime; }

   private:
    PlcRuntime *runtime;
    RServerLog *rLog;
    std::mutex mtx;
};

class RServer {
   public:
    RServer(bool standAloneMode, RServerLog *rLog) {
        this->standAloneMode = standAloneMode;
        this->rLog = rLog;
        server = new RServerRPC(rLog);
    }

    virtual int initServer(const std::string &udsFile);
    virtual int initServer();

    virtual int startServer();

    void setLogger(RServerLog *log) { this->rLog = log; }

    virtual ~RServer() {
        delete server;
    };

   private:
    bool standAloneMode;
    std::string udsAddress;
    RServerRPC *server;
    RServerLog *rLog;

    void udsCheck(const std::string &uds);
};

#endif  //_R_SERVER_H
