#ifndef _R_SERVER_H
#define _R_SERVER_H

#include <grpc/grpc.h>
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
    explicit RServerRPC() { this->runtime = nullptr; }

    void initRCore();

    virtual Status FunctionCall(ServerContext *context, const CallRequest *callRequest,
                                CallResponse *result) override;

    virtual ~RServerRPC() { delete this->runtime; }

   private:
    PlcRuntime *runtime;
    RServerLog rLog;
};

class RServer {
   public:
    explicit RServer(bool standAloneMode) { this->standAloneMode = standAloneMode; }

    virtual int initServer(const std::string &udsFile);
    virtual int initServer();

    virtual int startServer();

    virtual ~RServer() {};

   private:
    bool standAloneMode;
    std::string udsAddress;
    RServerRPC server;
    RServerLog rLog;

    void udsCheck(const std::string &uds);
};

#endif  //_R_SERVER_H
