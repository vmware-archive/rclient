#ifndef _RUTILS_H
#define _RUTILS_H

#include <string>

/* R header files */
#include <R.h>
#include <Rversion.h>
#include <Rembedded.h>
#include <Rinternals.h>
#include <R_ext/Parse.h>
#include <Rdefines.h>

// undef R marco to avoid conflict with protobuf
#undef Free
#undef length

enum class RServerLogLevel
{
	DEBUGS = 1,
	LOGS,
	WARNINGS,
	ERRORS,
	UNKNOWN = 99,
};

class RServerLog
{
public:
	RServerLog() : logLevel(RServerLogLevel::LOGS)
	{
		this->logLevel = RServerLogLevel::LOGS;
	}
	void log(RServerLogLevel lvl, const char *format, ...);
	void setLogLevel(RServerLogLevel level)
	{
		this->logLevel = level;
	}

	static const char *logLevelToString(RServerLogLevel level)
	{
		switch (level)
		{
		case RServerLogLevel::DEBUGS:
			return "R Server Debug";
		case RServerLogLevel::LOGS:
			return "R Server Log";
		case RServerLogLevel::WARNINGS:
			return "R Server Warning";
		case RServerLogLevel::ERRORS:
			return "R Server Error";
		default:
			break;
		}
		return "<unknown log level>";
	}

private:
	RServerLogLevel logLevel;
};

#endif //_RUTILS_H
