#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <raimd/md_msg.h>
#include <omm/ev_omm_client.h>
#include <omm/src_dir.h>
#include <raikv/zipf.h>

using namespace rai;
using namespace kv;
using namespace md;
using namespace omm;

static const uint32_t RATE_TIMER_SECS = 1;
static const uint64_t RATE_TIMER_ID   = 3, /* rate timer */
                      STOP_TIMER_ID   = 4; /* stop timer */

struct OmmDataCallback : public EvConnectionNotify, public OmmClientCB,
                         public EvTimerCallback {
  EvPoll      & poll;            /* poll loop data */
  EvOmmClient & client;          /* connection to rv */
  const char ** sub;             /* subject strings */
  size_t        sub_count,       /* count of sub[] */
                num_subs;
  uint64_t      msg_count,
                last_count,
                last_time,
                msg_bytes,
                last_bytes;
  bool          is_subscribed,   /* sub[] are subscribed */
                dump_hex,        /* print hex of message data */
                show_rate;       /* show rate of messages recvd */
  char        * subj_buf;
  uint32_t    * rand_schedule,
              * msg_recv;
  size_t      * msg_recv_bytes;
  UIntHashTab * subj_ht,
              * coll_ht;
  size_t        rand_range;
  uint32_t      max_time_secs;
  bool          use_random,
                use_zipf,
                quiet;
  uint64_t      seed1, seed2;

  OmmDataCallback( EvPoll &p,  EvOmmClient &c,  const char **s,  size_t cnt,
                   bool hex,  bool rate,  size_t n,  size_t rng,
                   bool zipf,  uint32_t secs, bool q, uint64_t s1, uint64_t s2 )
    : poll( p ), client( c ), sub( s ), sub_count( cnt ),
      num_subs( n ), msg_count( 0 ), last_count( 0 ), last_time( 0 ),
      msg_bytes( 0 ), last_bytes( 0 ), is_subscribed( false ), dump_hex( hex ),
      show_rate( rate ), subj_buf( 0 ), rand_schedule( 0 ), msg_recv( 0 ),
      msg_recv_bytes( 0 ), subj_ht( 0 ), coll_ht( 0 ), rand_range( rng ),
      max_time_secs( secs ), use_random( rng > cnt ), use_zipf( zipf ),
      quiet( q ) {
    this->seed1 = ( s1 == 0 ? current_monotonic_time_ns() : s1 );
    this->seed2 = ( s2 == 0 ? current_realtime_ns() : s2 );
  }

  void add_initial( size_t n,  size_t bytes ) {
    this->msg_recv[ n ] |= 1;
    this->msg_recv_bytes[ n ] += bytes;
  }
  void add_update( size_t n,  size_t bytes ) {
    this->msg_recv[ n ] += 1 << 1;
    this->msg_recv_bytes[ n ] += bytes;
  }
  /* after CONNECTED message */
  virtual void on_connect( EvSocket &conn ) noexcept;
  /* start sub[] with inbox reply */
  void make_random( void ) noexcept;
  void make_subject( size_t n,  const char *&s,  size_t &slen ) noexcept;
  void start_subscriptions( void ) noexcept;
  /* when signalled, unsubscribe */
  void on_unsubscribe( void ) noexcept;
  /* when disconnected */
  virtual void on_shutdown( EvSocket &conn,  const char *err,
                            size_t err_len ) noexcept;
  /* dict timeout */
  virtual bool timer_cb( uint64_t timer_id,  uint64_t event_id ) noexcept;
  /* message from network */
  virtual bool on_msg( const char *sub,  size_t sub_len,
                       uint32_t subj_hash,  RwfMsg &msg ) noexcept;
};

/* called after daemon responds with CONNECTED message */
void
OmmDataCallback::on_connect( EvSocket &conn ) noexcept
{
  int len = (int) conn.get_peer_address_strlen();
  printf( "Connected: %.*s\n", len, conn.peer_address.buf );
  if ( this->use_random ) {
    printf( "Random seed1: 0x%016lx seed2 0x%016lx\n", this->seed1,
            this->seed2 );
  }
  fflush( stdout );

  this->start_subscriptions();
}

void
OmmDataCallback::make_random( void ) noexcept
{
  BitSpace used;
  size_t   n     = this->num_subs,
           size  = this->sub_count * n,
           range = this->rand_range * this->sub_count;
  kv::rand::xoroshiro128plus rand;
  ZipfianGen<99, 100, kv::rand::xoroshiro128plus> zipf( range, rand ) ;
  if ( range < size )
    range = size;
  rand.static_init( this->seed1, this->seed2 );

  this->rand_schedule = (uint32_t *)
    ::malloc( sizeof( this->rand_schedule[ 0 ] ) * size );
  for ( size_t cnt = 0; cnt < size; ) {
    size_t n;
    if ( this->use_zipf )
      n = zipf.next() % range;
    else
      n = rand.next() % range;
    while ( used.test_set( n ) )
      n = ( n + 1 ) % range;
    this->rand_schedule[ cnt++ ] = n;
  }
}

void
OmmDataCallback::make_subject( size_t n,  const char *&s, size_t &slen ) noexcept
{
  if ( this->use_random ) {
    if ( this->rand_schedule == NULL )
      this->make_random();
    n = this->rand_schedule[ n ];
  }
  size_t j = n % this->sub_count;
  n   /= this->sub_count;
  s    = this->sub[ j ];
  slen = ::strlen( s );

  const char * p = ::strstr( s, "%d" );
  if ( this->num_subs > 1 || p != NULL ) {
    if ( this->subj_buf == NULL || slen + 12 > 1024 ) {
      size_t len = ( slen + 12 < 1024 ? 1024 : slen + 12 );
      this->subj_buf = (char *) ::realloc( this->subj_buf, len );
    }
    CatPtr cat( this->subj_buf );
    const char * e = &s[ slen ];
    if ( p == NULL ) {
      cat.x( s, e - s ).s( "." ).u( n );
    }
    else {
      cat.x( s, p - s ).u( n );
      p += 2;
      if ( p < e )
        cat.x( p, e - p );
    }
    s    = cat.start;
    slen = cat.end();
  }
}

/* start subscriptions from command line, inbox number indexes the sub[] */
void
OmmDataCallback::start_subscriptions( void ) noexcept
{
  if ( this->is_subscribed ) /* subscribing multiple times is allowed, */
    return;                  /* but must unsub multiple times as well */

  this->subj_ht = UIntHashTab::resize( NULL );
  this->coll_ht = UIntHashTab::resize( NULL );
  size_t n   = this->sub_count * this->num_subs;
  size_t sz  = sizeof( this->msg_recv[ 0 ] ) * n,
         sz2 = sizeof( this->msg_recv_bytes[ 0 ] ) * n;
  this->msg_recv = (uint32_t *) ::malloc( sz );
  this->msg_recv_bytes = (size_t *) ::malloc( sz2 );
  ::memset( this->msg_recv, 0, sz );
  ::memset( this->msg_recv_bytes, 0, sz2 );

  for ( n = 0; ; ) {
    for ( size_t j = 0; j < this->sub_count; j++, n++ ) {
      const char * subject;
      size_t       subject_len;

      this->make_subject( n, subject, subject_len );

      printf( "Subscribe \"%.*s\"\n", (int) subject_len, subject );
      /* subscribe with inbox reply */
      this->client.subscribe( subject, subject_len );

      uint32_t h = kv_crc_c( subject, subject_len, 0 ), val;
      size_t   pos;
      if ( this->subj_ht->find( h, pos, val ) ) {
        const char * coll_sub;
        size_t       coll_sublen;
        this->subj_ht->remove( pos );

        this->make_subject( val, coll_sub, coll_sublen );
        h = kv_djb( coll_sub, coll_sublen );
        if ( ! this->coll_ht->find( h, pos ) )
          this->coll_ht->set_rsz( this->coll_ht, h, pos, val );
        else {
          printf( "HT collision \"%.*s\", no update tracking\n",
                  (int) coll_sublen, coll_sub );
        }
        h = kv_djb( subject, subject_len );
        if ( ! this->coll_ht->find( h, pos ) )
          this->coll_ht->set_rsz( this->coll_ht, h, pos, n );
        else {
          printf( "HT collision \"%.*s\", no update tracking\n",
                  (int) subject_len, subject );
        }
      }
      else {
        this->subj_ht->set_rsz( this->subj_ht, h, pos, n );
      }
    }
    if ( n >= this->num_subs * this->sub_count )
      break;
  }
  fflush( stdout );

  if ( this->show_rate ) {
    this->last_time = this->poll.current_coarse_ns();
    this->poll.timer.add_timer_seconds( *this, RATE_TIMER_SECS,
                                        RATE_TIMER_ID, 0 );
  }
  if ( this->max_time_secs != 0 ) {
    this->poll.timer.add_timer_seconds( *this, this->max_time_secs,
                                        STOP_TIMER_ID, 0 );
  }
  this->is_subscribed = true;
}
/* if ctrl-c, program signalled, unsubscribe the subs */
void
OmmDataCallback::on_unsubscribe( void ) noexcept
{
  if ( ! this->is_subscribed )
    return;
  this->is_subscribed = false;
  for ( size_t n = 0; ; ) {
    for ( size_t j = 0; j < this->sub_count; j++, n++ ) {
      const char * subject;
      size_t       subject_len;
      this->make_subject( n, subject, subject_len );
      printf( "Unsubscribe \"%.*s\" initial=%u update=%u bytes=%u\n",
              (int) subject_len, subject, this->msg_recv[ n ] & 1,
              this->msg_recv[ n ] >> 1, (uint32_t) this->msg_recv_bytes[ n ] );
      /* subscribe with inbox reply */
      this->client.unsubscribe( subject, subject_len );
    }
    if ( n >= this->num_subs * this->sub_count )
      break;
  }
  fflush( stdout );
}
/* dict timer expired */
bool
OmmDataCallback::timer_cb( uint64_t timer_id,  uint64_t ) noexcept
{
  if ( timer_id == RATE_TIMER_ID ) {
    uint64_t ival_ns = this->poll.now_ns - this->last_time,
             count   = this->msg_count - this->last_count,
             bytes   = this->msg_bytes - this->last_bytes;
    if ( this->last_count < this->msg_count ) {
      printf( "%.2f m/s %.2f mbit/s\n",
              (double) count * 1000000000.0 / (double) ival_ns,
              (double) bytes * 8.0 * 1000.0 / ival_ns );
    }
    this->last_time  += ival_ns;
    this->last_count += count;
    this->last_bytes += bytes;
    return true;
  }
  if ( timer_id == STOP_TIMER_ID ) {
    this->on_unsubscribe();
    if ( this->poll.quit == 0 )
      this->poll.quit = 1; /* causes poll loop to exit */
  }
  return false; /* return false to disable recurrent timer */
}

void
OmmDataCallback::on_shutdown( EvSocket &conn,  const char *err,
                              size_t errlen ) noexcept
{ 
  int len = (int) conn.get_peer_address_strlen();
  printf( "Shutdown: %.*s %.*s\n",
          len, conn.peer_address.buf, (int) errlen, err );
  /* if disconnected by tcp, usually a reconnect protocol, but this just exits*/
  if ( this->poll.quit == 0 )
    this->poll.quit = 1; /* causes poll loop to exit */
}

bool
OmmDataCallback::on_msg( const char *sub,  size_t sub_len,
                         uint32_t subj_hash,  RwfMsg &msg ) noexcept
{
  RwfMsgHdr & hdr     = msg.msg;
  size_t      pub_len = msg.msg_end - msg.msg_off,
              pos;
  uint32_t    val;

  if ( this->subj_ht->find( subj_hash, pos, val ) ) {
    if ( hdr.msg_class == UPDATE_MSG_CLASS )
      this->add_update( val, pub_len );
    else if ( hdr.msg_class == REFRESH_MSG_CLASS )
      this->add_initial( val, pub_len );
  }
  else if ( this->coll_ht->find( kv_djb( sub, sub_len ), pos, val ) ) {
    if ( hdr.msg_class == UPDATE_MSG_CLASS )
      this->add_update( val, pub_len );
    else if ( hdr.msg_class == REFRESH_MSG_CLASS )
      this->add_initial( val, pub_len );
  }
  if ( this->show_rate ) {
    this->msg_count++;
    this->msg_bytes += pub_len;
    return true;
  }
  if ( this->quiet )
    return true;

  printf( "## %.*s (stream_id: %u):\n", (int) sub_len, sub, hdr.stream_id );
  MDOutput mout;
  msg.print( &mout );
  if ( this->dump_hex )
    mout.print_hex( &msg );
  return true;
}

static const char *
get_arg( int &x, int argc, const char *argv[], int b, const char *f,
         const char *g, const char *def ) noexcept
{
  for ( int i = 1; i < argc - b; i++ ) {
    if ( ::strcmp( f, argv[ i ] ) == 0 || ::strcmp( g, argv[ i ] ) == 0 ) {
      if ( x < i + b + 1 )
        x = i + b + 1;
      return argv[ i + b ];
    }
  }
  return def; /* default value */
}

int
main( int argc, const char *argv[] )
{ 
  SignalHandler sighndl;
  int x = 1;
  const char * host    = get_arg( x, argc, argv, 1, "-d", "-host","127.0.0.1" ),
             * app_nm  = get_arg( x, argc, argv, 1, "-n", "-name","omm_client"),
             * app_id  = get_arg( x, argc, argv, 1, "-i", "-id", "256" ),
             * user    = get_arg( x, argc, argv, 1, "-u", "-user", "nobody" ),
             * pass    = get_arg( x, argc, argv, 1, "-z", "-pass", NULL ),
             * inst    = get_arg( x, argc, argv, 1, "-I", "-instance", NULL ),
             * token   = get_arg( x, argc, argv, 1, "-t", "-token", NULL ),
             * path    = get_arg( x, argc, argv, 1, "-c", "-cfile", NULL ),
             * nodict  = get_arg( x, argc, argv, 0, "-x", "-nodict", NULL ),
             * dump    = get_arg( x, argc, argv, 0, "-e", "-hex", NULL ),
             * rate    = get_arg( x, argc, argv, 0, "-r", "-rate", NULL ),
             * sub_cnt = get_arg( x, argc, argv, 1, "-k", "-count", NULL ),
             * zipf    = get_arg( x, argc, argv, 0, "-Z", "-zipf", NULL ),
             * time    = get_arg( x, argc, argv, 1, "-t", "-secs", NULL ),
             * debug   = get_arg( x, argc, argv, 0, "-g", "-debug", NULL ),
             * quiet   = get_arg( x, argc, argv, 0, "-q", "-quiet", NULL ),
             * s1      = get_arg( x, argc, argv, 1, "-S", "-seed1", NULL ),
             * s2      = get_arg( x, argc, argv, 1, "-T", "-seed2", NULL ),
             * help    = get_arg( x, argc, argv, 0, "-h", "-help", 0 );
  int first_sub = x, idle_count = 0;
  size_t cnt = 1, range = 0, secs = 0;
  uint64_t seed1 = 0, seed2 = 0;

  if ( help != NULL ) {
  help:;
    fprintf( stderr,
 "%s [-d host] [-n name] [-u user] [-c cfile_path] [-x] [-e] subject ...\n"
             "  -d host     = daemon port to connect\n"
             "  -n name     = application name\n"
             "  -i id       = application id\n"
             "  -u user     = user name\n"
             "  -z pass     = user pass\n"
             "  -I instance = instance id\n"
             "  -t token    = user token\n"
             "  -c cfile    = if loading dictionary from files\n"
             "  -x          = don't load a dictionary\n"
             "  -e          = show hex dump of messages\n"
             "  -r          = show rate of messages\n"
             "  -k          = subscribe to numeric subjects (subject.%%d)\n"
             "  -Z          = use zipf(0.99) distribution\n"
             "  -t secs     = stop after seconds expire\n"
             "  -q          = quiet, don't print messages\n"
             "  -S hex      = random seed1\n"
             "  -T hex      = random seed2\n"
             "  subject     = subject to subscribe\n", argv[ 0 ] );
    return 1;
  }
  if ( first_sub >= argc ) {
    fprintf( stderr, "No subjects subscribed\n" );
    goto help;
  }
  if ( debug != NULL )
    omm_debug = 1;
  bool valid = true;
  if ( sub_cnt != NULL ) {
    size_t       len = ::strlen( sub_cnt );
    const char * p   = ::strchr( sub_cnt, ':' ),
               * e   = &sub_cnt[ len ];
    if ( p == NULL )
      p = e;
    valid = valid_uint64( sub_cnt, p - sub_cnt );
    if ( &p[ 1 ] < e )
      valid = valid_uint64( &p[ 1 ], e - &p[ 1 ] );

    if ( valid ) {
      cnt = string_to_uint64( sub_cnt, p - sub_cnt );
      if ( &p[ 1 ] < e )
        range = string_to_uint64( &p[ 1 ], e - &p[ 1 ] );
      else
        range = cnt;
      if ( cnt == 0 )
        valid = false;
    }
    if ( ! valid ) {
      fprintf( stderr, "Invalid -k/-count\n" );
      goto help;
    }
  }
  if ( time != NULL ) {
    MDStamp stamp;
    if( stamp.parse( time, ::strlen( time ), true ) != 0 )
      valid = false;
    if ( ! valid ) {
      fprintf( stderr, "Invalid -t/-secs\n" );
      goto help;
    }
    secs = stamp.seconds();
  }
  if ( s1 != NULL || s2 != NULL ) {
    if ( s1 == NULL || ! valid_uint64( s1, ::strlen( s1 ) ) ||
         s2 == NULL || ! valid_uint64( s2, ::strlen( s2 ) ) ) {
      fprintf( stderr, "Invalid -S/-T/-seed1/-seed2\n" );
      goto help;
    }
    seed1 = string_to_uint64( s1, ::strlen( s1 ) );
    seed2 = string_to_uint64( s2, ::strlen( s2 ) );
  }

  EvPoll    poll;
  poll.init( 5, false );

  EvOmmClientParameters param( host, app_nm, app_id, user, pass, inst, token );
  OmmDict               dict;
  OmmSourceDB           source_db;
  EvOmmClient           conn( poll, dict, source_db );
  OmmDataCallback       data( poll, conn, &argv[ first_sub ], argc - first_sub,
                              dump != NULL, rate != NULL, cnt, range,
                              zipf != NULL, secs, quiet != NULL, seed1, seed2 );
  if ( nodict != NULL )
    conn.no_dictionary = true;
  /* load dictionary if present */
  if ( ! conn.no_dictionary ) {
    conn.have_dictionary = dict.load_cfiles( path );
    if ( conn.have_dictionary )
      printf( "Loaded dictionary from cfiles\n" );
  }
  md_init_auto_unpack();
  
  if ( ! conn.connect( param, &data, &data ) ) {
    fprintf( stderr, "unable to connect to %s\n", host );
    return 1;
  }

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

