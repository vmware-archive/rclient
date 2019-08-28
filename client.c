/*------------------------------------------------------------------------------
 *
 * Copyright (c) 2016-Present Pivotal Software, Inc
 *
 *------------------------------------------------------------------------------
 */
#include <stdlib.h>
#include <assert.h>

#include "comm_connectivity.h"
#include "server.h"
#include "misc.h"
#include "rcall.h"

int main(int argc UNUSED, char **argv UNUSED) {
	int status;

	// Make sure we have the same length in GPDB and server side
	assert(sizeof(float) == 4 && sizeof(double) == 8);
	set_signal_handlers();

	// Initialize R
	plc_elog(LOG, "Client start to listen execution");
	status = r_init();

	plcconn_global = start_server();
	if (status == 0) {
		receive_loop();
	} else {
		plc_raise_delayed_error(plcconn_global);
	}

	plc_elog(LOG, "Client has finished execution");
	return 0;
}



