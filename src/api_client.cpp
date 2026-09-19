/* Copyright (c) 2026 Rai Technology.  All rights reserved.
 *  http://www.raitechnology.com
 *
 * omm_api_client -- sample / test for the ommapi consumer api: connect,
 * list the source directory, subscribe subjects (SERVICE.REC.RIC), print
 * what arrives, optionally as the converted sass RVMSG. */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <signal.h>
#include <raimd/md_msg.h>
#include <raimd/rv_msg.h>
#include <omm/ommapi.h>

using namespace rai;
using namespace md;

static volatile int quit;
static int          show_sass, quiet, snapshot;
static uint32_t     msg_count, max_msgs;

static void
sig_handler( int )
{
  quit = 1;
}

static const char *
class_str( ommMsgClass c )
{
  switch ( c ) {
    case OMM_MSG_REFRESH:        return "REFRESH";
    case OMM_MSG_STATUS:         return "STATUS";
    case OMM_MSG_UPDATE:         return "UPDATE";
    case OMM_MSG_TRANSPORT_UP:   return "TRANSPORT_UP";
    case OMM_MSG_TRANSPORT_DOWN: return "TRANSPORT_DOWN";
    default:                     return "?";
  }
}

static void
on_msg( ommEvent,  ommMsg msg,  void * )
{
  const char   * subject = "", * text = "";
  ommMsgClass    cls = OMM_MSG_STATUS;
  ommStreamState ss = OMM_STREAM_UNSPECIFIED;
  ommDataState   ds = OMM_DATA_NO_CHANGE;
  uint8_t        code = 0;
  uint16_t       msg_type = 0, rec_status = 0;
  int            has_seq = 0;
  uint32_t       seq = 0;

  ommMsg_GetSubject( msg, &subject );
  ommMsg_GetMsgClass( msg, &cls );
  ommMsg_GetState( msg, &ss, &ds, &code );
  ommMsg_GetStatusText( msg, &text );
  ommMsg_GetSassHeader( msg, &msg_type, &rec_status );
  ommMsg_GetSeqNum( msg, &has_seq, &seq );

  if ( cls == OMM_MSG_TRANSPORT_UP || cls == OMM_MSG_TRANSPORT_DOWN ) {
    printf( "## %s: %s\n", class_str( cls ), text );
    fflush( stdout );
    return;
  }
  msg_count++;
  if ( quiet )
    return;
  printf( "## %s %s stream=%u data=%u code=%u sass(MSG_TYPE=%u REC_STATUS=%u)",
          subject, class_str( cls ), ss, ds, code, msg_type, rec_status );
  if ( has_seq )
    printf( " seq=%u", seq );
  if ( text[ 0 ] != '\0' )
    printf( " \"%s\"", text );
  printf( "\n" );
  if ( show_sass ) {
    const void * buf;
    uint32_t     len;
    omm_status   st = ommMsg_GetSassMsg( msg, &buf, &len );
    if ( st != OMM_OK )
      printf( "  sass convert: %s\n", ommStatus_GetText( st ) );
    else {
      MDMsgMem mem;
      MDMsg  * m = MDMsg::unpack( (void *) buf, 0, len, RVMSG_TYPE_ID, NULL,
                                  mem );
      if ( m != NULL ) {
        MDOutput out;
        m->print( &out );
      }
    }
  }
  else {
    ommMsg_Print( msg, stdout );
  }
  fflush( stdout );
}

int
main( int argc,  char *argv[] )
{
  ommTransportParams parms;
  omm_InitTransportParams( &parms );
  const char * subjects[ 64 ];
  int          nsubj = 0;
  double       secs  = 0;

  for ( int i = 1; i < argc; i++ ) {
    if ( ::strcmp( argv[ i ], "-d" ) == 0 && i + 1 < argc )
      parms.daemon = argv[ ++i ];
    else if ( ::strcmp( argv[ i ], "-u" ) == 0 && i + 1 < argc )
      parms.user = argv[ ++i ];
    else if ( ::strcmp( argv[ i ], "-c" ) == 0 && i + 1 < argc )
      parms.dict_path = argv[ ++i ];
    else if ( ::strcmp( argv[ i ], "-x" ) == 0 )
      parms.no_dictionary = 1;
    else if ( ::strcmp( argv[ i ], "-s" ) == 0 )
      show_sass = 1;
    else if ( ::strcmp( argv[ i ], "-S" ) == 0 )
      snapshot = 1;
    else if ( ::strcmp( argv[ i ], "-q" ) == 0 )
      quiet = 1;
    else if ( ::strcmp( argv[ i ], "-n" ) == 0 && i + 1 < argc )
      max_msgs = (uint32_t) atoi( argv[ ++i ] );
    else if ( ::strcmp( argv[ i ], "-t" ) == 0 && i + 1 < argc )
      secs = atof( argv[ ++i ] );
    else if ( ::strcmp( argv[ i ], "-h" ) == 0 || argv[ i ][ 0 ] == '-' ) {
      fprintf( stderr,
        "%s [-d host:port] [-u user] [-c cfile_path] [-x] [-s] [-S] [-q]\n"
        "   [-n msgs] [-t secs] SERVICE.REC.RIC ...\n"
        "  -d  provider, default 127.0.0.1:14002\n"
        "  -c  load RDM dictionary from cfiles instead of downloading\n"
        "  -x  no dictionary\n"
        "  -s  print the sass-form RVMSG conversion instead of the rwf\n"
        "  -S  snapshot (refresh only) instead of streaming\n"
        "  -q  count only\n"
        "  -n  quit after msgs messages\n"
        "  -t  quit after secs\n", argv[ 0 ] );
      return 1;
    }
    else if ( nsubj < 64 )
      subjects[ nsubj++ ] = argv[ i ];
  }
  signal( SIGINT, sig_handler );
  signal( SIGTERM, sig_handler );

  omm_status st = omm_Open();
  if ( st != OMM_OK ) {
    fprintf( stderr, "omm_Open: %s\n", ommStatus_GetText( st ) );
    return 1;
  }
  ommTransport t;
  ommQueue     q = 0;
  ommEvent     state_ev = 0, ev[ 64 ];
  st = ommTransport_Create( &t, &parms );
  if ( st != OMM_OK ) {
    fprintf( stderr, "ommTransport_Create: %s\n", ommStatus_GetText( st ) );
    return 1;
  }
  ommQueue_Create( &q );
  ommTransport_SetStateListener( t, q, on_msg, NULL, &state_ev );

  printf( "ommapi %s: connecting to %s\n", omm_Version(), parms.daemon != NULL ?
          parms.daemon : "127.0.0.1:14002" );
  st = ommTransport_WaitConnected( t, 10.0 );
  if ( st != OMM_OK ) {
    fprintf( stderr, "connect: %s\n", ommStatus_GetText( st ) );
    return 1;
  }
  uint32_t nsrc = 0;
  ommTransport_GetSourceCount( t, &nsrc );
  printf( "%u source%s:\n", nsrc, nsrc == 1 ? "" : "s" );
  for ( uint32_t i = 0; i < nsrc; i++ ) {
    const char * name; uint32_t id; int up;
    if ( ommTransport_GetSource( t, i, &name, &id, &up ) == OMM_OK )
      printf( "  %s (id %u) %s\n", name, id, up ? "up" : "down" );
  }
  int have = 0;
  ommTransport_HaveDictionary( t, &have );
  printf( "dictionary: %s\n", have ? "yes" : "no" );

  for ( int i = 0; i < nsubj; i++ ) {
    const char * svc, * ric; uint8_t dom;
    st = ommTransport_MatchSubject( t, subjects[ i ], &svc, &dom, &ric );
    if ( st != OMM_OK )
      printf( "%s: %s\n", subjects[ i ], ommStatus_GetText( st ) );
    else
      printf( "%s -> service %s domain %u ric %s\n", subjects[ i ], svc, dom,
              ric );
    st = ommEvent_CreateListener( &ev[ i ], q, t, subjects[ i ],
                                  snapshot ? OMM_LISTEN_SNAPSHOT
                                           : OMM_LISTEN_STREAMING,
                                  on_msg, NULL );
    if ( st != OMM_OK )
      printf( "listen %s: %s\n", subjects[ i ], ommStatus_GetText( st ) );
  }
  double t0 = 0;
  struct timespec ts;
  clock_gettime( CLOCK_MONOTONIC, &ts );
  t0 = ts.tv_sec + ts.tv_nsec / 1e9;
  while ( ! quit ) {
    ommQueue_TimedDispatch( q, 0.1 );
    if ( max_msgs != 0 && msg_count >= max_msgs )
      break;
    if ( secs > 0 ) {
      clock_gettime( CLOCK_MONOTONIC, &ts );
      if ( ts.tv_sec + ts.tv_nsec / 1e9 - t0 >= secs )
        break;
    }
  }
  printf( "%u messages\n", msg_count );
  for ( int i = 0; i < nsubj; i++ )
    ommEvent_Destroy( ev[ i ] );
  ommEvent_Destroy( state_ev );
  ommTransport_Destroy( t );
  ommQueue_Destroy( q );
  return 0;
}
