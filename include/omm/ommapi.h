/* Copyright (c) 2026 Rai Technology.  All rights reserved.
 *  http://www.raitechnology.com
 *
 * ommapi.h -- thread-safe OMM consumer api, shaped like the tibrv 7 api that
 * sassrv/rv7api.h provides for RV (same queue / event / transport model, so
 * a program written for one maps onto the other):
 *
 *   omm_Open()                         start the api (one epoll thread)
 *   ommTransport_Create( &t, &parms )  connect to an OMM provider / ADS:
 *                                      login, source directory download,
 *                                      dictionary download
 *   ommQueue_Create( &q )              an event queue the app thread drains
 *   ommEvent_CreateListener( &e, q, t, "SERVICE.REC.RIC", flags, cb, cl )
 *                                      an item stream; the subject is
 *                                      <service>.<sector>.<ric>, matched
 *                                      against the downloaded directory
 *                                      (sector REC = market price domain,
 *                                      MBO, MBP, ... as rdm_sector_str[])
 *   ommEvent_CreateTimer( &e, q, cb, secs, cl )
 *   ommQueue_TimedDispatch( q, secs )  run callbacks on this thread
 *
 * Callbacks receive an ommMsg: the RWF message (refresh / update / status)
 * with its stream state, plus a converted sass-form RVMSG (MSG_TYPE, SEQ_NO,
 * REC_STATUS + the field list) for consumers that speak sass.  A message is
 * valid for the duration of the callback unless ommMsg_Detach() is called.
 *
 * Every call may be made from any thread.  Status codes are omm_status;
 * ommStatus_GetText() describes them.
 *
 * v1 is consumer only: no posting (publish) and no provider side. */
#ifndef __rai_omm__ommapi_h__
#define __rai_omm__ommapi_h__

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef uint32_t ommId;
typedef ommId    ommTransport,
                 ommQueue,
                 ommQueueGroup,
                 ommEvent,
                 ommDispatcher,
                 ommDispatchable; /* a queue or a queue group */
typedef struct omm_msg_s * ommMsg;

#define OMM_INVALID_ID      0
#define OMM_DEFAULT_QUEUE   1
#define OMM_WAIT_FOREVER    -1.0
#define OMM_NO_WAIT         0.0

typedef enum {
  OMM_OK                  = 0,
  OMM_INIT_FAILURE        = 1,
  OMM_NOT_INITIALIZED     = 2,
  OMM_INVALID_TRANSPORT   = 3,
  OMM_INVALID_QUEUE       = 4,
  OMM_INVALID_QUEUE_GROUP = 5,
  OMM_INVALID_EVENT       = 6,
  OMM_INVALID_DISPATCHER  = 7,
  OMM_INVALID_ARG         = 8,
  OMM_INVALID_SUBJECT     = 9,  /* not SERVICE.SECTOR.RIC */
  OMM_NO_SOURCE           = 10, /* no directory source matches the subject */
  OMM_NOT_CONNECTED       = 11,
  OMM_TIMEOUT             = 12,
  OMM_NOT_SUPPORTED       = 13,
  OMM_NO_MEMORY           = 14,
  OMM_INVALID_MSG         = 15,
  OMM_CONVERT_FAILED      = 16  /* RWF -> sass conversion failed */
} omm_status;

/* transport connection parameters (strings are copied) */
typedef struct {
  const char * daemon;       /* host[:port], default 127.0.0.1:14002 */
  const char * user;         /* login name, default the process user */
  const char * app_name;     /* ApplicationName login attribute */
  const char * app_id;       /* ApplicationId, default "256" */
  const char * password;
  const char * instance_id;
  const char * token;        /* AuthenticationToken */
  const char * dict_path;    /* load the RDM dictionary from these cfiles
                              * instead of downloading (NULL = download) */
  int          no_dictionary;/* don't download / load a dictionary */
  double       reconnect_secs; /* retry interval after a disconnect,
                                * 0 = default (1s), < 0 = don't reconnect */
} ommTransportParams;

/* fill defaults */
void omm_InitTransportParams( ommTransportParams *p );

/* ---- api ------------------------------------------------------------------ */
omm_status   omm_Open( void );
omm_status   omm_Close( void );
const char * omm_Version( void );
const char * ommStatus_GetText( omm_status status );

/* ---- transport ------------------------------------------------------------ */
omm_status ommTransport_Create( ommTransport *tport,
                                const ommTransportParams *parms );
omm_status ommTransport_Destroy( ommTransport tport );
/* connected = login + directory (+ dictionary) complete */
omm_status ommTransport_IsConnected( ommTransport tport,  int *connected );
/* block until connected or timeout secs (OMM_WAIT_FOREVER) */
omm_status ommTransport_WaitConnected( ommTransport tport,  double timeout );
omm_status ommTransport_HaveDictionary( ommTransport tport,  int *have_dict );
omm_status ommTransport_GetDaemon( ommTransport tport,  const char **daemon );
omm_status ommTransport_GetUser( ommTransport tport,  const char **user );
omm_status ommTransport_SetDescription( ommTransport tport,  const char *d );
omm_status ommTransport_GetDescription( ommTransport tport,  const char **d );
/* the source directory: services and whether they are up */
omm_status ommTransport_GetSourceCount( ommTransport tport,  uint32_t *count );
omm_status ommTransport_GetSource( ommTransport tport,  uint32_t idx,
                                   const char **service_name,
                                   uint32_t *service_id,  int *is_up );
/* resolve SERVICE.SECTOR.RIC against the directory: the matched service
 * name, the rdm domain of the sector (6 = market price) and the ric */
omm_status ommTransport_MatchSubject( ommTransport tport,  const char *subject,
                                      const char **service_name,
                                      uint8_t *domain,  const char **ric );
/* connection state changes are delivered as events on a queue: the callback
 * gets a msg with ommMsg_GetMsgClass() == OMM_MSG_TRANSPORT_UP / _DOWN and
 * ommMsg_GetStatusText() describing it */
omm_status ommTransport_SetStateListener( ommTransport tport,  ommQueue queue,
                                          void ( *cb )( ommEvent event,
                                                        ommMsg msg,
                                                        void *closure ),
                                          const void *closure,
                                          ommEvent *event );

/* ---- queues --------------------------------------------------------------- */
typedef void ( *ommQueueOnComplete )( ommQueue queue,  void *closure );

omm_status ommQueue_Create( ommQueue *queue );
omm_status ommQueue_Destroy( ommQueue queue );
omm_status ommQueue_DestroyEx( ommQueue queue,  ommQueueOnComplete cb,
                               const void *closure );
/* dispatch all pending events (waiting up to timeout secs for the first);
 * OMM_TIMEOUT when nothing arrived */
omm_status ommQueue_TimedDispatch( ommQueue queue,  double timeout );
omm_status ommQueue_TimedDispatchOneEvent( ommQueue queue,  double timeout );
omm_status ommQueue_Dispatch( ommQueue queue );   /* wait forever */
omm_status ommQueue_Poll( ommQueue queue );       /* no wait */
omm_status ommQueue_GetCount( ommQueue queue,  uint32_t *num_events );
omm_status ommQueue_SetPriority( ommQueue queue,  uint32_t priority );
omm_status ommQueue_GetPriority( ommQueue queue,  uint32_t *priority );
omm_status ommQueue_SetName( ommQueue queue,  const char *name );
omm_status ommQueue_GetName( ommQueue queue,  const char **name );

omm_status ommQueueGroup_Create( ommQueueGroup *grp );
omm_status ommQueueGroup_Destroy( ommQueueGroup grp );
omm_status ommQueueGroup_Add( ommQueueGroup grp,  ommQueue queue );
omm_status ommQueueGroup_Remove( ommQueueGroup grp,  ommQueue queue );
omm_status ommQueueGroup_TimedDispatch( ommQueueGroup grp,  double timeout );

/* a thread that dispatches a queue or group until destroyed */
omm_status ommDispatcher_Create( ommDispatcher *disp,  ommDispatchable able,
                                 double idle_timeout );
omm_status ommDispatcher_Join( ommDispatcher disp );
omm_status ommDispatcher_Destroy( ommDispatcher disp );
omm_status ommDispatcher_SetName( ommDispatcher disp,  const char *name );
omm_status ommDispatcher_GetName( ommDispatcher disp,  const char **name );

/* ---- events --------------------------------------------------------------- */
typedef enum {
  OMM_TIMER_EVENT     = 1,
  OMM_LISTEN_EVENT    = 3,
  OMM_TRANSPORT_EVENT = 6  /* state listener */
} ommEventType;

/* listener flags */
#define OMM_LISTEN_STREAMING  0x1 /* refresh + updates until destroyed */
#define OMM_LISTEN_SNAPSHOT   0x2 /* refresh only, the stream closes after */
#define OMM_LISTEN_NO_REFRESH 0x4 /* updates only, no solicited refresh */

typedef void ( *ommEventCallback )( ommEvent event,  ommMsg msg,
                                    void *closure );

/* subject SERVICE.SECTOR.RIC; OMM_NO_SOURCE when the directory has no
 * source for it (the directory is available once the transport is
 * connected; a listener created before that is resolved at connect and
 * gets a status message if it does not match) */
omm_status ommEvent_CreateListener( ommEvent *event,  ommQueue queue,
                                    ommTransport tport,  const char *subject,
                                    int flags,  ommEventCallback cb,
                                    const void *closure );
omm_status ommEvent_CreateTimer( ommEvent *event,  ommQueue queue,
                                 ommEventCallback cb,  double interval,
                                 const void *closure );
omm_status ommEvent_Destroy( ommEvent event );
omm_status ommEvent_GetType( ommEvent event,  ommEventType *type );
omm_status ommEvent_GetQueue( ommEvent event,  ommQueue *queue );
omm_status ommEvent_GetListenerSubject( ommEvent event,
                                        const char **subject );
omm_status ommEvent_GetListenerTransport( ommEvent event,
                                          ommTransport *tport );
omm_status ommEvent_GetTimerInterval( ommEvent event,  double *interval );
omm_status ommEvent_ResetTimerInterval( ommEvent event,  double interval );

/* ---- messages ------------------------------------------------------------- */
/* message classes: the RWF msg class of a stream message (rwf values), plus
 * the api's own transport state notifications */
typedef enum {
  OMM_MSG_REFRESH        = 2,
  OMM_MSG_STATUS         = 3,
  OMM_MSG_UPDATE         = 4,
  OMM_MSG_TRANSPORT_UP   = 100,
  OMM_MSG_TRANSPORT_DOWN = 101
} ommMsgClass;

/* rdm stream / data states (omm_flags.h values) */
typedef enum {
  OMM_STREAM_UNSPECIFIED    = 0,
  OMM_STREAM_OPEN           = 1,
  OMM_STREAM_NON_STREAMING  = 2,
  OMM_STREAM_CLOSED_RECOVER = 3,
  OMM_STREAM_CLOSED         = 4,
  OMM_STREAM_REDIRECTED     = 5
} ommStreamState;

typedef enum {
  OMM_DATA_NO_CHANGE = 0,
  OMM_DATA_OK        = 1,
  OMM_DATA_SUSPECT   = 2
} ommDataState;

omm_status ommMsg_GetSubject( ommMsg msg,  const char **subject );
omm_status ommMsg_GetEvent( ommMsg msg,  ommEvent *event );
omm_status ommMsg_GetMsgClass( ommMsg msg,  ommMsgClass *msg_class );
/* stream state: from a refresh / status; updates report the last known */
omm_status ommMsg_GetState( ommMsg msg,  ommStreamState *stream_state,
                            ommDataState *data_state,  uint8_t *status_code );
omm_status ommMsg_GetStatusText( ommMsg msg,  const char **text );
omm_status ommMsg_IsRefreshComplete( ommMsg msg,  int *complete );
omm_status ommMsg_IsSolicited( ommMsg msg,  int *solicited );
omm_status ommMsg_GetSeqNum( ommMsg msg,  int *has_seq,  uint32_t *seq_num );
omm_status ommMsg_GetRecvTime( ommMsg msg,  int64_t *recv_ns );
/* the RWF message bytes (valid while the msg is) */
omm_status ommMsg_GetRwf( ommMsg msg,  const void **buf,  uint32_t *len );
/* the sass-form RVMSG: MSG_TYPE (INITIAL / UPDATE / ... from the rwf class),
 * SEQ_NO when present, REC_STATUS (from the status code), then the field
 * list with dictionary names.  Converted on first call, owned by the msg */
omm_status ommMsg_GetSassMsg( ommMsg msg,  const void **buf,  uint32_t *len );
/* the sass header the conversion produces / would produce */
omm_status ommMsg_GetSassHeader( ommMsg msg,  uint16_t *msg_type,
                                 uint16_t *rec_status );
/* keep the message past the callback; ommMsg_Destroy releases it */
omm_status ommMsg_Detach( ommMsg msg );
omm_status ommMsg_Destroy( ommMsg msg );
/* print the field list to fp (stdout when NULL) */
omm_status ommMsg_Print( ommMsg msg,  void *fp );

#ifdef __cplusplus
}
#endif
#endif
