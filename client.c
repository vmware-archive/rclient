/*------------------------------------------------------------------------------
 *
 * Copyright (c) 2016-Present Pivotal Software, Inc
 *
 *------------------------------------------------------------------------------
 */
#include <stdlib.h>
#include <assert.h>

#include "common/comm_channel.h"
#include "common/comm_utils.h"
#include "common/comm_connectivity.h"
#include "common/comm_server.h"
#include "rcall.h"

int main(int argc UNUSED, char **argv UNUSED) {
	int sock;
	plcConn *conn;
	int status;

	assert(sizeof(int8) == 1);
	assert(sizeof(int16) == 2);
	assert(sizeof(int32) == 4);
	assert(sizeof(uint32) == 4);
	assert(sizeof(int64) == 8);
	assert(sizeof(float4) == 4);
	assert(sizeof(float8) == 8);

	set_signal_handlers();

	/* do not overwrite, if the CLIENT_NAME has already set */
	setenv("CLIENT_LANGUAGE", "rclient", 0);

	// Bind the socket and start listening the port
	sock = start_listener();

	// Initialize R
	plc_elog(LOG, "Client start to listen execution");
	status = r_init();

	connection_wait(sock);
	conn = connection_init(sock);
	if (status == 0) {
		receive_loop(handle_call, conn);
	} else {
		plc_raise_delayed_error(conn);
	}

	plc_elog(LOG, "Client has finished execution");
	return 0;
}



