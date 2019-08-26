/*------------------------------------------------------------------------------
 *
 * Copyright (c) 2016-Present Pivotal Software, Inc
 *
 *------------------------------------------------------------------------------
 */
#ifndef PLC_RCALL_H
#define PLC_RCALL_H

#include "messages/messages.h"
#include "comm_connectivity.h"

#define UNUSED __attribute__ (( unused ))

// Global connection object
extern plcConn *plcconn_global;

// Global execution termination flag
int plc_is_execution_terminated;

// Processing of the Greenplum function call
void handle_call(plcMsgCallreq *req);

// Initialization of R module
int r_init(void);

void raise_execution_error(const char *format, ...);

void plc_raise_delayed_error();

void receive_loop();

#endif /* PLC_RCALL_H */
