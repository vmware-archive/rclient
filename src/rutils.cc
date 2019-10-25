#include <cstdio>
#include <cstdarg>
#include <cstdlib>

#include "rutils.hh"

void RServerLog::log(RServerLogLevel lvl, const char *format, ...) {
    FILE *out = stdout;
    if (lvl >= RServerLogLevel::ERRORS) {
        out = stderr;
    }
    if (lvl >= this->logLevel) {
        va_list ap;
        fprintf(out, "plcontainer server log: %s:", logLevelToString(lvl));
        va_start(ap, format);
        vfprintf(out, format, ap);
        va_end(ap);
        fprintf(out, "\n");
        fflush(out);
    }
    if (lvl >= RServerLogLevel::ERRORS) {
        exit(1);
    }
}
