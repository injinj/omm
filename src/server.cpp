#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#if ! defined( _MSC_VER ) && ! defined( __MINGW32__ )
#include <unistd.h>
#include <pthread.h>
#include <signal.h>
#else
#include <raikv/win.h>
#endif
#include <omm/ev_omm.h>
#include <omm/ev_omm_client.h>
#include <omm/src_dir.h>
#include <raikv/ev_net.h>
#include <raikv/mainloop.h>
#include <raimd/md_dict.h>
#include <raimd/cfile.h>
#include <raimd/app_a.h>
#include <raimd/enum_def.h>

using namespace rai;
using namespace kv;
using namespace omm;
using namespace md;

struct Args : public MainLoopVars { /* argv[] parsed args */
  const char * path,
             * publisher;
  OmmDict      dict;
  int          omm_port;
  bool         test;
  Args() : path( 0 ), publisher( 0 ), omm_port( 0 ), test( false ) {}
};

struct Loop : public MainLoop<Args> {
  Loop( EvShm &m,  Args &args,  size_t num, bool (*ini)( void * ) ) :
    MainLoop<Args>( m, args, num, ini ) {}

  EvOmmListen * omm_sv;
  bool omm_init( void ) {
    this->omm_sv = new ( ::malloc( sizeof( EvOmmListen ) ) )
      EvOmmListen( this->poll, this->r.dict );
    if ( this->omm_sv->listen( NULL, this->r.omm_port, this->r.tcp_opts ) != 0 ) {
      fprintf( stderr, "unable to open listen socket on %d\n", this->r.omm_port );
      return false;
    }
    if ( this->r.dict.rdm_dict != NULL ) {
      printf( "\n" );
      print_dict_info( this->r.dict.rdm_dict, "RWFFld", "RWFEnum" );
    }
    if ( this->r.test )
      this->add_test_sources();
    if ( this->r.publisher != NULL )
      this->add_publisher( this->r.publisher );
    return true;
  }

  bool init( void ) {
    if ( this->thr_num == 0 ) {
      printf( "omm_daemon:           %d\n", this->r.omm_port );
    }
    int cnt = this->omm_init();
    if ( this->thr_num == 0 )
      fflush( stdout );
    return cnt > 0;
  }

  static bool initialize( void *me ) noexcept;

  void add_test_sources( void ) noexcept;

  void add_publisher( const char *host ) noexcept;
};

bool
Loop::initialize( void *me ) noexcept
{
  return ((Loop *) me)->init();
}

void
Loop::add_test_sources( void ) noexcept
{
  static const char * dict[ 2 ] = { "RWFFld", "RWFEnum" };
  static uint8_t cap[ 4 ] = { LOGIN_DOMAIN, SOURCE_DOMAIN, DICTIONARY_DOMAIN,
                              MARKET_PRICE_DOMAIN };
  static RwfQos  qos      = { QOS_TIME_REALTIME, QOS_RATE_TICK_BY_TICK, 0, 0, 0 };

  char buf[ 1024 ];
  MDMsgMem mem;
  RwfMapWriter map( mem, this->omm_sv->dict.rdm_dict, buf, sizeof( buf ) );
  RwfState     state = { STREAM_STATE_OPEN, DATA_STATE_OK, 0, { "OK", 2 } };

  RwfFilterListWriter
    & fil = map.add_filter_list( MAP_ADD_ENTRY, 100, MD_UINT );

  fil.add_element_list( FILTER_SET_ENTRY, DIR_SVC_INFO_ID )
     .append_string( NAME        , "RSF" )
     .append_string( VEND        , "Test" )
     .append_uint  ( IS_SRC      , 0 )
     .append_array ( CAPAB       , cap , 4, MD_UINT )
     .append_array ( DICT_PROV   , dict, 2 )
     .append_array ( DICT_USED   , dict, 2 )
     .append_array ( QOS         , &qos, 1 )
     .append_uint  ( SUP_QOS_RNG , 0 )
     .append_string( ITEM_LST    , "_ITEM_LIST" )
     .append_uint  ( SUP_OOB_SNAP, 1 )
     .append_uint  ( ACC_CONS_STA, 0 )
   .end_element_list();

  fil.add_element_list( FILTER_SET_ENTRY, DIR_SVC_STATE_ID )
     .append_uint  ( SVC_STATE, 1 )
     .append_uint  ( ACC_REQ  , 1 )
     .append_state ( STAT     , state )
   .end_element_list();

  fil.add_element_list( FILTER_SET_ENTRY, DIR_SVC_LOAD_ID )
     .append_uint( OPEN_LIM , 1000 * 1000 )
     .append_uint( OPEN_WIN , 1000 )
     .append_uint( LOAD_FACT, 0 )
   .end_element_list();

  fil.add_map( FILTER_SET_ENTRY, DIR_SVC_LINK_ID )
     .set_key_type( MD_STRING )
     .add_element_list( MAP_ADD_ENTRY, "link" )
       .append_uint  ( TYPE      , LINK_INTERACTIVE ) /* 1 = Interactive, 2 = Broadcast */
       .append_uint  ( LINK_STATE, LINK_UP )          /* 1 = Up, 0 = Down */
       .append_uint  ( LINK_CODE , LINK_OK )          /* 1 = 0k, 2 = recovery start, 3 = recovery completed */
       .append_string( TEXT      , "Link state is up" )
     .end_map();
  map.end_map();

  RwfMsg *m = RwfMsg::unpack_map( map.buf, 0, map.off, RWF_MAP_TYPE_ID, NULL, mem );
  if ( is_omm_debug ) {
    MDOutput mout;
    if ( m != NULL )
      m->print( &mout );
  }
  this->omm_sv->add_source( *m );
}

void
Loop::add_publisher( const char *host ) noexcept
{
  EvOmmClientParameters param( host, "omm_server", NULL, "server", NULL, NULL, NULL );
  EvOmmClient         * conn = new ( ::malloc( sizeof( EvOmmClient ) ) )
                               EvOmmClient( this->omm_sv->poll, this->omm_sv->dict );
  conn->have_dictionary = ( conn->dict.rdm_dict != NULL );
  if ( ! conn->connect( param, NULL, NULL, this->omm_sv->x_source_db ) )
    fprintf( stderr, "unable to connect to %s\n", host );
}

int
main( int argc, const char *argv[] )
{
  EvShm shm( "omm_server" );
  Args  r;

  r.no_threads   = true;
  r.no_reuseport = true;
  r.no_map       = true;
  r.no_default   = true;
  r.all          = true;
  r.add_desc( "  -c path = dictionary cfile_path    ($cfile_path)" );
  r.add_desc( "  -r port = listen omm port          (14002)" );
  r.add_desc( "  -g      = turn on debug" );
  r.add_desc( "  -t      = add test sources" );
  r.add_desc( "  -p host = connect to publisher" );
  if ( ! r.parse_args( argc, argv ) )
    return 1;
  if ( shm.open( r.map_name, r.db_num ) != 0 )
    return 1;
  printf( "omm_version:          " kv_stringify( OMM_VER ) "\n" );
  shm.print();
  r.omm_port  = r.parse_port( argc, argv, "-r", "14002" );
  r.publisher = r.get_arg( argc, argv, 1, "-p", NULL );
  r.path      = r.get_arg( argc, argv, 1, "-c", ".", "cfile_path" );
  r.test      = r.bool_arg( argc, argv, 0, "-t", NULL, NULL );
  if ( r.bool_arg( argc, argv, 0, "-g", NULL, NULL ) )
    omm_debug = 1;

  if ( r.path != NULL && r.dict.load_cfiles( r.path ) ) {
    if ( r.dict.rdm_dict != NULL )
      printf( "rdm dictionary:       %s (RDMFieldDictionary enumtype.def)\n",
              r.path );
    if ( r.dict.cfile_dict != NULL )
      printf( "sass dictionary:      %s (tss_fields.cf tss_records.cf)\n",
              r.path );
  }

  Runner<Args, Loop> runner( r, shm, Loop::initialize );
  if ( r.thr_error == 0 )
    return 0;
  return 1;
}
#if 0
int
main( void )
{ 
  SignalHandler sighndl;
  EvPoll poll;
  EvOmmListen test( poll );
  int idle_count = 0; 
  poll.init( 5, false );
  
  if ( test.listen( "127.0.0.1", 14002, DEFAULT_TCP_LISTEN_OPTS ) != 0 )
    return 1;
  sighndl.install();
  for (;;) {
    /* loop 5 times before quiting, time to flush writes */
    if ( poll.quit >= 5 && idle_count > 0 )
      break;
    /* dispatch network events */
    int idle = poll.dispatch();
    if ( idle == EvPoll::DISPATCH_IDLE )
      idle_count++;
    else
      idle_count = 0;
    /* wait for network events */ 
    poll.wait( idle_count > 2 ? 100 : 0 );
    if ( sighndl.signaled )
      poll.quit++;
  }
  return 0;
}
#endif
