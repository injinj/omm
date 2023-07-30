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
#include <raimd/md_dict.h>
#include <raimd/cfile.h>
#include <raimd/app_a.h>
#include <raimd/enum_def.h>
#include <omm/test_pub.h>

using namespace rai;
using namespace kv;
using namespace omm;
using namespace md;

struct Args : public MainLoopVars { /* argv[] parsed args */
  OmmDict      dict;
  OmmSourceDB  source_db;
  const char * path,
             * publisher;
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
      EvOmmListen( this->poll, this->r.dict, this->r.source_db );
    if ( this->omm_sv->listen( NULL, this->r.omm_port, this->r.tcp_opts ) != 0 ) {
      fprintf( stderr, "unable to open listen socket on %d\n", this->r.omm_port );
      return false;
    }
    if ( this->r.dict.rdm_dict != NULL ) {
      printf( "\n" );
      print_dict_info( this->r.dict.rdm_dict, "RWFFld", "RWFEnum" );
    }
    if ( this->r.test ) {
      TestPublish *p = new ( ::malloc( sizeof( TestPublish ) ) )
        TestPublish( this->omm_sv );
      p->add_test_source( "RSF", 100 );
      p->add_test_source( "RDF", 101 );
      p->add_test_source( "OPR", 102 );
      p->start();
    }
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

  void add_publisher( const char *host ) noexcept;
};

bool
Loop::initialize( void *me ) noexcept
{
  return ((Loop *) me)->init();
}

void
Loop::add_publisher( const char *host ) noexcept
{
  EvOmmClientParameters param( host, "omm_server", NULL, "server", NULL, NULL, NULL );
  EvOmmClient         * conn;
  conn = new ( ::malloc( sizeof( EvOmmClient ) ) )
       EvOmmClient( this->omm_sv->poll, this->omm_sv->dict,
                    this->omm_sv->x_source_db );
  conn->have_dictionary = ( conn->dict.rdm_dict != NULL );
  if ( ! conn->connect( param, NULL, NULL ) )
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
