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
#include <raikv/mainloop.h>
#include <raikv/logger.h>
#include <raimd/md_dict.h>
#include <raimd/cfile.h>
#include <raimd/app_a.h>
#include <raimd/enum_def.h>
#include <omm/test_pub.h>
#include <omm/test_replay.h>
#include <omm/rv_submgr.h>
#include <sassrv/ev_rv_client.h>

using namespace rai;
using namespace kv;
using namespace omm;
using namespace sassrv;
using namespace md;

struct PrintSrcs : public OmmSrcListener {
  EvPoll      & poll;
  OmmSourceDB & source_db;
  PrintSrcs( EvPoll &p,  OmmSourceDB & db ) : poll( p ), source_db( db ) {}
  virtual void on_src_change( void ) noexcept;
};

void
PrintSrcs::on_src_change( void ) noexcept
{
  if ( ! this->poll.quit )
    this->source_db.print_sources();
}

struct Args : public MainLoopVars { /* argv[] parsed args */
  OmmDict               dict;
  OmmSourceDB           source_db;
  const char          * path,
                      * feed,
                      * file_name,
                      * prefix,
                      * msg_fmt,
                      * log_file;
  EvOmmClientParameters ads;
  EvRvClientParameters  rv;
  int                   omm_port,
                        ft_weight;
  bool                  test;
  Args() : path( 0 ), feed( 0 ), file_name( 0 ), prefix( 0 ),
           msg_fmt( 0 ), log_file( 0 ),
           ads( NULL, "omm_server", "256", NULL, NULL, NULL ),
           rv( NULL ),
           omm_port( 0 ), ft_weight( 0 ), test( false ) {}
};

template<class Sock, class Param, class ClientCB>
struct EvReconnect : public EvTimerCallback, public EvConnectionNotify {
  Sock               & client;
  Param              & param;
  EvConnectionNotify * notify;
  ClientCB           * cb;
  int                  reconnect_count;

  void * operator new( size_t, void *ptr ) { return ptr; }
  EvReconnect( Sock &cl,  Param &p,  EvConnectionNotify *n,  ClientCB *c )
    : client( cl ), param( p ), notify( n ), cb( c ), reconnect_count( 0 ) {}

  void connect( void ) {
    if ( ! this->client.connect( this->param, this, this->cb ) ) {
      if ( this->reconnect_count++ == 0 ) {
        fprintf( stderr, "Reconnect 5 second interval\n" );
        this->param.opts &= ~OPT_VERBOSE;
      }
      this->client.poll.timer.add_timer_seconds( *this, 5, 0, 0 );
    }
  }
  virtual void on_connect( EvSocket &conn ) noexcept {
    int len = (int) conn.get_peer_address_strlen();
    if ( this->notify != NULL )
      this->notify->on_connect( conn );
    else
      printf( "Client connected: %.*s\n", len, conn.peer_address.buf );
  }
  virtual void on_shutdown( EvSocket &conn,  const char *err,
                            size_t errlen ) noexcept {
    if ( this->notify != NULL )
      this->notify->on_shutdown( conn, err, errlen );
    else {
      int len = (int) conn.get_peer_address_strlen();
      printf( "Client shutdown: %.*s %.*s\n",
              len, conn.peer_address.buf, (int) errlen, err );
    }
    if ( ! this->client.poll.quit )
      this->connect();
  }
  virtual bool timer_cb( uint64_t ,  uint64_t ) noexcept {
    if ( ! this->client.poll.quit )
      this->connect();
    return false;
  }
};
typedef EvReconnect<EvOmmClient, EvOmmClientParameters,
                    OmmClientCB> OmmReconnect;
typedef EvReconnect<EvRvClient, EvRvClientParameters,
                    RvClientCB> RvReconnect;

struct Loop : public MainLoop<Args> {
  Loop( EvShm &m,  Args &args,  size_t num )
    : MainLoop( m, args, num ), omm_sv( 0 ), omm_client( 0 ),
      omm_conn( 0 ), rv_client( 0 ), rv_conn( 0 ), rv_submgr( 0 ), log( 0 ),
      print_srcs( this->poll, args.source_db ) {}

  EvOmmListen  * omm_sv;
  EvOmmClient  * omm_client;
  OmmReconnect * omm_conn;
  EvRvClient   * rv_client;
  RvReconnect  * rv_conn;
  RvOmmSubmgr  * rv_submgr;
  Logger       * log;
  PrintSrcs      print_srcs;

  bool omm_init( void ) noexcept;

  virtual bool initialize( void ) noexcept {
    return this->omm_init();
  }
  virtual bool finish( void ) noexcept {
    if ( this->rv_submgr == NULL )
      return true;
    if ( ! this->rv_submgr->is_stopped )
      this->rv_submgr->on_stop();
    return this->rv_submgr->is_finished;
  }
  void add_publisher( void ) noexcept;
  void add_rvclient( void ) noexcept;
  void on_log( void ) noexcept;
  void log_output( int stream,  uint64_t stamp,  size_t len,
                   const char *buf ) noexcept;
};

bool
Loop::omm_init( void ) noexcept
{
  bool added_sources = false;
  if ( this->r.log_file != NULL ) {
    this->log = Logger::create();
    if ( this->log->output_log_file( this->r.log_file ) != 0 ) {
      perror( this->r.log_file );
      return false;
    }
    this->log->start_ev( this->poll );
  }
  this->r.source_db.add_source_listener( &this->print_srcs );
  printf( "omm_version:          " kv_stringify( OMM_VER ) "\n" );
  if ( r.path != NULL && r.dict.load_cfiles( r.path ) ) {
    if ( r.dict.rdm_dict != NULL )
      printf( "rdm dictionary:       %s (RDMFieldDictionary enumtype.def)\n",
              r.path );
    if ( r.dict.cfile_dict != NULL )
      printf( "sass dictionary:      %s (tss_fields.cf tss_records.cf)\n",
              r.path );
    if ( r.dict.flist_dict != NULL )
      printf( "flist dictionary:     %s (flistmapping)\n",
              r.path );
  }

  if ( this->r.omm_port != 0 ) {
    this->omm_sv = new ( aligned_malloc( sizeof( EvOmmListen ) ) )
      EvOmmListen( this->poll, this->r.dict, this->r.source_db );
    if ( this->omm_sv->listen( NULL, this->r.omm_port,
                               this->r.tcp_opts ) != 0 ) {
      fprintf( stderr, "Unable to open listen socket on %d\n",
               this->r.omm_port );
      return false;
    }
    else if ( this->r.omm_port != 0 )
      printf( "omm_daemon:           %d\n", this->r.omm_port );
  }

  if ( this->r.dict.rdm_dict != NULL ) {
    printf( "\n" );
    print_dict_info( this->r.dict.rdm_dict, "RWFFld", "RWFEnum" );
  }

  if ( this->r.test ) {
    TestPublish *p = new ( aligned_malloc( sizeof( TestPublish ) ) )
      TestPublish( this->poll, this->r.dict, this->r.source_db );
    p->add_test_source( this->r.feed, 100 );
    p->start();
    added_sources = true;
  }

  if ( this->r.file_name != NULL && ! this->r.test ) {
    TestReplay *p = new ( aligned_malloc( sizeof( TestReplay ) ) )
      TestReplay( this->poll, this->r.dict, this->r.source_db );
    p->add_replay_file( this->r.feed, 100, this->r.file_name );
    p->start();
    added_sources = true;
  }

  if ( this->r.ads.daemon != NULL )
    this->add_publisher();
  if ( this->r.rv.daemon != NULL || this->r.rv.network != NULL ||
       this->r.rv.service != NULL )
    this->add_rvclient();

  if ( added_sources )
    this->r.source_db.notify_source_change();
  return true;
}

void
Loop::add_publisher( void ) noexcept
{
  this->omm_client = new ( aligned_malloc( sizeof( EvOmmClient ) ) )
         EvOmmClient( this->poll, this->r.dict, this->r.source_db );
  this->omm_client->have_dictionary =
    ( this->omm_client->dict.rdm_dict != NULL );
  this->omm_conn =
    new ( ::malloc( sizeof( OmmReconnect ) ) )
      OmmReconnect( *this->omm_client, this->r.ads, NULL, NULL );
  this->omm_conn->connect();
}

void
Loop::add_rvclient( void ) noexcept
{
  this->rv_client = new ( aligned_malloc( sizeof( EvRvClient ) ) )
    EvRvClient( this->poll );
  this->rv_submgr = new ( aligned_malloc( sizeof( RvOmmSubmgr ) ) )
    RvOmmSubmgr( this->poll, *this->rv_client, this->r.dict,
                 this->r.source_db, this->r.ft_weight, this->r.prefix,
                 this->r.msg_fmt );
  if ( this->r.feed != NULL && this->r.feed[ 0 ] != '\0' ) {
    this->rv_submgr->feed = &this->r.feed;
    this->rv_submgr->feed_count = 1;
  }
  this->rv_conn =
    new ( ::malloc( sizeof( RvReconnect ) ) )
      RvReconnect( *this->rv_client, this->r.rv, this->rv_submgr,
                   this->rv_submgr );
  this->rv_conn->connect();
/*  if ( ! this->rv_client->connect( this->r.rv, this->rv_submgr,
                                   this->rv_submgr ) )
    fprintf( stderr, "Unable to connect to rv\n" );*/
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
  r.add_desc( "  -c path   = dictionary cfile_path  ($cfile_path)" );
  r.add_desc( "  -o port   = listen omm port" );
  r.add_desc( "  -d daemon = rv daemon" );
  r.add_desc( "  -n net    = rv network" );
  r.add_desc( "  -s svc    = rv service" );
  r.add_desc( "  -u user   = rv user                (omm_server)" );
  r.add_desc( "  -a pref   = rv subject prefix      (_TIC.)" );
  r.add_desc( "  -m fmt    = rv message format      (TIB_QFORM)" );
  r.add_desc( "  -l file   = log file" );
  r.add_desc( "  -g        = turn on debug" );
  r.add_desc( "  -t        = add test sources" );
  r.add_desc( "  -w weight = rv ft weight" );
  r.add_desc( "  -p host   = connect to publisher, interactive or bcast" );
  r.add_desc( "  -i app_id = identify app           (256)" );
  r.add_desc( "  -f file   = replay file name" );
  r.add_desc( "  -r feed   = feed prefix            (RSF)" );
  if ( ! r.parse_args( argc, argv ) )
    return 1;
  if ( shm.open( r.map_name, r.db_num ) != 0 )
    return 1;
  shm.print();
  r.omm_port   = r.parse_port( argc, argv, "-o", NULL );
  r.rv.daemon  = r.get_arg( argc, argv, 1, "-d", NULL );
  r.rv.network = r.get_arg( argc, argv, 1, "-n", NULL );
  r.rv.service = r.get_arg( argc, argv, 1, "-s", NULL );
  r.rv.userid  = r.get_arg( argc, argv, 1, "-u", "omm_server" );
  r.prefix     = r.get_arg( argc, argv, 1, "-a", "_TIC." );
  r.msg_fmt    = r.get_arg( argc, argv, 1, "-m", "TIB_QFORM" );
  r.log_file   = r.get_arg( argc, argv, 1, "-l", NULL );
  r.ft_weight  = r.int_arg( argc, argv, 1, "-w", "20", NULL );
  r.ads.daemon = r.get_arg( argc, argv, 1, "-p", NULL );
  r.ads.app_id = r.get_arg( argc, argv, 1, "-i", "256" );
  r.ads.user   = r.rv.userid;
  r.path       = r.get_arg( argc, argv, 1, "-c", ".", "cfile_path" );
  r.test       = r.bool_arg( argc, argv, 0, "-t", NULL, NULL );
  r.feed       = r.get_arg( argc, argv, 1, "-r", "RSF" );
  r.file_name  = r.get_arg( argc, argv, 1, "-f", NULL );
  if ( r.bool_arg( argc, argv, 0, "-g", NULL, NULL ) )
    omm_debug = 1;

  Runner<Args, Loop> runner( r, shm );
  if ( r.thr_error == 0 )
    return 0;
  return 1;
}
