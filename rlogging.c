/*------------------------------------------------------------------------------
 *
 * Copyright (c) 2016-Present Pivotal Software, Inc
 *
 *------------------------------------------------------------------------------
 */
#include <R.h>
#include <Rinternals.h>

#include "common/comm_channel.h"
#include "common/comm_dummy.h"
#include "common/messages/messages.h"
#include "rcall.h"
#include "rlogging.h"

static SEXP plr_output(volatile int, SEXP args);

SEXP plr_debug(SEXP args) {
	return plr_output(DEBUG2, args);
}

SEXP plr_log(SEXP args) {
	return plr_output(LOG, args);
}

SEXP plr_info(SEXP args) {
	return plr_output(INFO, args);
}

SEXP plr_notice(SEXP args) {
	return plr_output(NOTICE, args);
}

SEXP plr_warning(SEXP args) {
	return plr_output(WARNING, args);
}

SEXP plr_error(SEXP args) {
	return plr_output(ERROR, args);
}

SEXP plr_fatal(SEXP args) {
	return plr_output(FATAL, args);
}

static SEXP plr_output(volatile int level, SEXP args) {
	plcMsgLog *msg;

	if (plc_is_execution_terminated == 0) {
		char *str_msg = strdup(CHAR(asChar(args)));


		if (level >= ERROR)
			plc_is_execution_terminated = 1;

		msg = palloc(sizeof(plcMsgLog));
		msg->msgtype = MT_LOG;
		msg->level = level;
		msg->message = str_msg;

		plcontainer_channel_send(plcconn_global, (plcMessage *) msg);

		pfree(msg);
		pfree(str_msg);
	}

	/*
	 * return a legal object so the interpreter will continue on its merry way
	 */
	return R_NilValue;
}
