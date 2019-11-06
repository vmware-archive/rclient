#include "rutils.hh"

RServerLog::RServerLog(RServerWorkingMode mode, std::string filename) {
    this->mode = mode;
    this->logLevel = RServerLogLevel::LOGS;

    switch (mode) {
        case RServerWorkingMode::STANDALONG:
            this->printer = new StdLogPrinter();
            break;
        case RServerWorkingMode::CONTAINER: {
            this->localBuffer.reserve(MAX_SERVER_LOG_BUFFER_SIZE + 5);
            this->printer = new BufferLogPrinter(&this->localBuffer);
        } break;
        case RServerWorkingMode::CONTAINERDEBUG:
            this->printer = new FileLogPrinter(filename);
            break;
        default: {
            std::string err = "Unknow mode for log module";
            this->printer = new StdLogPrinter();
            this->printer->print(err);
        } break;
    }
}

void RServerLog::log(RServerLogLevel lvl, const char *format, ...) {
    char buf[MAX_MESSAGE_LINE_LENGTH];
    std::string log = "R Server Logs, ";
    if (lvl >= this->logLevel) {
        va_list ap;
        va_start(ap, format);
        vsnprintf(buf, sizeof(buf), format, ap);
        va_end(ap);

        log = log + this->logLevelToString(lvl) + std::string(buf);
        this->printer->print(log);
    }
    switch (lvl) {
        case RServerLogLevel::ERRORS:
            throw RServerErrorException(log);
            break;
        case RServerLogLevel::WARNINGS:
            throw RServerWarningException(log);
            break;
        default:
            break;
    }
}

void FileLogPrinter::print(std::string &logs) { this->file << logs + "\n"; }

void StdLogPrinter::print(std::string &logs) {
    fprintf(stdout, logs.c_str());
    fprintf(stdout, "\n");
    fflush(stdout);
}

void BufferLogPrinter::print(std::string &logs) {
    if (this->bufferRef->size() <= MAX_SERVER_LOG_BUFFER_SIZE - MAX_MESSAGE_LINE_LENGTH) {
        this->bufferRef->append(logs + "\n");
    }
}

std::string RServerLog::logLevelToString(RServerLogLevel level) {
    switch (level) {
        case RServerLogLevel::DEBUGS:
            return "DEBUG, ";
        case RServerLogLevel::LOGS:
            return "LOG, ";
        case RServerLogLevel::WARNINGS:
            return "WARNING, ";
        case RServerLogLevel::ERRORS:
            return "ERROR, ";
        default:
            break;
    }
    return "<unknown log level>, ";
}
