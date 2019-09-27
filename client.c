/*------------------------------------------------------------------------------
 *
 * Copyright (c) 2016-Present Pivotal Software, Inc
 *
 *------------------------------------------------------------------------------
 */
#include <stdlib.h>
#include <assert.h>
#include <unistd.h>

#include "common/comm_connectivity.h"
#include "common/comm_dummy.h"
#include "server/server.h"
#include "rcall.h"

int main(int argc, char **argv) {
	int status;

	// Make sure we have the same length in GPDB and server side
	assert(sizeof(float) == 4 && sizeof(double) == 8);
	set_signal_handlers();

	// Initialize R
	plc_elog(LOG, "Server start to listen execution");
	status = r_init();
	if (argc <= 1) {
		plcconn_global = start_server(NULL);
	} else {
		plc_elog(LOG, "Server start in stand alone mode in %s", argv[1]);
		plcconn_global = start_server(argv[1]);
	}

	if (status == 0) {
		receive_loop();
	} else {
		plc_raise_delayed_error(plcconn_global);
	}

	plc_elog(LOG, "Server has finished execution");
	return 0;
}



