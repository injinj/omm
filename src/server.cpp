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

struct Args : public MainLoopVars { /* argv[] parsed args */
  OmmDict               dict;
  OmmSourceDB           source_db;
  const char          * path,
                      * replay,
                      * file_name,
                      * prefix,
                      * msg_fmt,
                      * log_file;
  EvOmmClientParameters ads;
  EvRvClientParameters  rv;
  int                   omm_port,
                        ft_weight;
  bool                  test;
  Args() : path( 0 ), replay( 0 ), file_name( 0 ), prefix( 0 ),
           msg_fmt( 0 ), log_file( 0 ),
           ads( NULL, "omm_server", "256", NULL, NULL, NULL ),
           rv( NULL ),
           omm_port( 0 ), ft_weight( 0 ), test( false ) {}
};

struct Loop : public MainLoop<Args> {
  Loop( EvShm &m,  Args &args,  size_t num )
    : MainLoop( m, args, num ), omm_sv( 0 ), omm_client( 0 ),
      rv_client( 0 ), rv_submgr( 0 ) {}

  EvOmmListen * omm_sv;
  EvOmmClient * omm_client;
  EvRvClient  * rv_client;
  RvOmmSubmgr * rv_submgr;
  Logger      * log;

  bool omm_init( void ) {
    if ( this->r.log_file != NULL ) {
      this->log = Logger::create();
      if ( this->log->output_log_file( this->r.log_file ) != 0 ) {
        perror( this->r.log_file );
        return false;
      }
      this->log->start_ev( this->poll );
    }
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
        fprintf( stderr, "unable to open listen socket on %d\n",
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
      p->add_test_source( "RSF", 100 );
      p->add_test_source( "RDF", 101 );
      p->add_test_source( "OPR", 102 );
      p->start();
    }

    if ( this->r.file_name != NULL ) {
      TestReplay *p = new ( aligned_malloc( sizeof( TestReplay ) ) )
        TestReplay( this->poll, this->r.dict, this->r.source_db );
      p->add_replay_file( this->r.replay, 103, this->r.file_name );
      p->start();
    }

    if ( this->r.ads.daemon != NULL )
      this->add_publisher();
    if ( this->r.rv.daemon != NULL || this->r.rv.network != NULL ||
         this->r.rv.service != NULL )
      this->add_rvclient();
    this->r.source_db.print_sources();
    return true;
  }

  virtual bool initialize( void ) noexcept {
    bool ok = this->omm_init();
    fflush( stdout );
    return ok;
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

void
Loop::add_publisher( void ) noexcept
{
  if ( this->omm_client == NULL ) {
    this->omm_client = new ( aligned_malloc( sizeof( EvOmmClient ) ) )
           EvOmmClient( this->poll, this->r.dict, this->r.source_db );
    this->omm_client->have_dictionary =
      ( this->omm_client->dict.rdm_dict != NULL );
  }
  if ( ! this->omm_client->connect( this->r.ads, NULL, NULL ) )
    fprintf( stderr, "unable to connect to %s\n", this->r.ads.daemon );
}

void
Loop::add_rvclient( void ) noexcept
{
  if ( this->rv_client == NULL ) {
    this->rv_client = new ( aligned_malloc( sizeof( EvRvClient ) ) )
      EvRvClient( this->poll );
    this->rv_submgr = new ( aligned_malloc( sizeof( RvOmmSubmgr ) ) )
      RvOmmSubmgr( this->poll, *this->rv_client, this->r.dict,
                   this->r.ft_weight, this->r.prefix, this->r.msg_fmt );
  }
  if ( ! this->rv_client->connect( this->r.rv, this->rv_submgr,
                                   this->rv_submgr ) )
    fprintf( stderr, "unable to connect to rv\n" );
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
  r.add_desc( "  -m fmt    = rv message format      (SASS_QFORM)" );
  r.add_desc( "  -l file   = log file" );
  r.add_desc( "  -g        = turn on debug" );
  r.add_desc( "  -t        = add test sources" );
  r.add_desc( "  -w weight = rv ft weight" );
  r.add_desc( "  -p host   = connect to publisher, interactive or bcast" );
  r.add_desc( "  -i app_id = identify app           (256)" );
  r.add_desc( "  -f file   = replay file name" );
  r.add_desc( "  -r feed   = replay feed name       (XXX)" );
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
  r.msg_fmt    = r.get_arg( argc, argv, 1, "-m", "SASS_QFORM" );
  r.log_file   = r.get_arg( argc, argv, 1, "-l", NULL );
  r.ft_weight  = r.int_arg( argc, argv, 1, "-w", "20", NULL );
  r.ads.daemon = r.get_arg( argc, argv, 1, "-p", NULL );
  r.ads.app_id = r.get_arg( argc, argv, 1, "-i", "256" );
  r.ads.user   = r.rv.userid;
  r.path       = r.get_arg( argc, argv, 1, "-c", ".", "cfile_path" );
  r.test       = r.bool_arg( argc, argv, 0, "-t", NULL, NULL );
  r.replay     = r.get_arg( argc, argv, 1, "-r", "XXX" );
  r.file_name  = r.get_arg( argc, argv, 1, "-f", NULL );
  if ( r.bool_arg( argc, argv, 0, "-g", NULL, NULL ) )
    omm_debug = 1;

  Runner<Args, Loop> runner( r, shm );
  if ( r.thr_error == 0 )
    return 0;
  return 1;
}
