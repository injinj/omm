/* Copyright (c) 2026 Rai Technology.  All rights reserved.
 *  http://www.raitechnology.com
 *
 * omm_api.cpp -- the thread-safe OMM consumer api (include/omm/ommapi.h).
 *
 * Layout follows sassrv's rv7_api.cpp: kv::ApiCore owns the epoll thread,
 * the id registry, the queues, timers and dispatchers; this file adds the
 * transport (an EvOmmClient plus its directory and dictionary), the
 * listeners (item streams, matched to listeners by subject) and the message
 * handle.  Api calls that touch the connection cross to the epoll thread as
 * ApiOp records over the ApiPipe. */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <pthread.h>
#include <raikv/ev_api_queue.h>
#include <raikv/key_hash.h>
#include <raimd/md_msg.h>
#include <raimd/md_dict.h>
#include <raimd/rv_msg.h>
#include <raimd/rwf_msg.h>
#include <raimd/sass.h>
#include <omm/ev_omm_client.h>
#include <omm/src_dir.h>
#include <omm/ommapi.h>

#ifndef OMM_VER
#define OMM_VER dev
#endif
#define STR2( s ) #s
#define STR( s ) STR2( s )

using namespace rai;
using namespace kv;
using namespace md;
using namespace omm;

namespace ommapi {

enum ElemType {
  OMM_ELEM_TIMER       = API_ELEM_TIMER,
  OMM_ELEM_LISTENER    = API_ELEM_LISTENER,
  OMM_ELEM_QUEUE       = API_ELEM_QUEUE,
  OMM_ELEM_QUEUE_GROUP = API_ELEM_QUEUE_GROUP,
  OMM_ELEM_TRANSPORT   = API_ELEM_TRANSPORT,
  OMM_ELEM_DISPATCHER  = API_ELEM_DISPATCHER
};

struct Omm_API;
struct api_Transport;
struct api_Listener;
struct api_Msg;
typedef ApiTimer api_Timer;

static const double DEFAULT_RECONNECT_SECS = 1.0;
static const uint64_t RECONNECT_TIMER_ID   = 0x0771;

/* ---- listener ------------------------------------------------------------- */

struct api_Listener {
  api_Listener   * next, * back;
  char           * subject;
  const void     * cl;
  ommEventCallback cb;
  ommEvent         id;
  ommQueue         queue;
  ommTransport     tport;
  uint32_t         hash;
  uint16_t         len;
  int              flags;      /* OMM_LISTEN_* */
  uint8_t          stream_state, /* last state seen on the stream */
                   data_state;
  bool             done,       /* snapshot delivered / stream closed */
                   is_state;   /* transport state listener, not a stream */

  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }
  api_Listener( Omm_API &,  ommId i ) : next( 0 ), back( 0 ), subject( 0 ),
    cl( 0 ), cb( 0 ), id( i ), queue( 0 ), tport( 0 ), hash( 0 ), len( 0 ),
    flags( 0 ), stream_state( OMM_STREAM_UNSPECIFIED ),
    data_state( OMM_DATA_NO_CHANGE ), done( false ), is_state( false ) {}
  ~api_Listener() { if ( this->subject != NULL ) ::free( this->subject ); }
  bool same_subject( const char *s,  size_t l ) const {
    return this->len == l && ::memcmp( this->subject, s, l ) == 0;
  }
};
typedef DLinkList< api_Listener > ListenerList;

/* subject hash -> listeners (several listeners may share a subject) */
struct ListenerHT {
  ListenerList * ht;
  size_t         mask, count;
  ListenerHT() : ht( 0 ), mask( 0 ), count( 0 ) {}
  void init( size_t sz ) {
    this->mask  = sz - 1;
    this->count = 0;
    sz *= sizeof( this->ht[ 0 ] );
    this->ht = (ListenerList *) ::malloc( sz );
    ::memset( (void *) this->ht, 0, sz );
  }
  void resize( void ) {
    size_t sz = this->mask + 1;
    ListenerList * oht = this->ht;
    this->init( oht == NULL ? 16 : sz * 2 );
    if ( oht != NULL ) {
      for ( size_t i = 0; i < sz; i++ )
        while ( ! oht[ i ].is_empty() )
          this->push( oht[ i ].pop_hd() );
      ::free( oht );
    }
  }
  void push( api_Listener *l ) {
    if ( this->count >= this->mask )
      this->resize();
    this->ht[ l->hash & this->mask ].push_tl( l );
    this->count++;
  }
  void remove( api_Listener *l ) {
    this->ht[ l->hash & this->mask ].pop( l );
    this->count--;
  }
  ListenerList *bucket( uint32_t h ) {
    return this->ht == NULL ? NULL : &this->ht[ h & this->mask ];
  }
  /* another live (not done) *streaming* listener on the same subject,
   * other than x: those share the one item stream; snapshots have their
   * own stream each */
  bool other_live( api_Listener *x ) {
    ListenerList * b = this->bucket( x->hash );
    if ( b != NULL ) {
      for ( api_Listener *l = b->hd; l != NULL; l = l->next )
        if ( l != x && ! l->done && ! l->is_state &&
             ( l->flags & OMM_LISTEN_SNAPSHOT ) == 0 &&
             l->hash == x->hash && l->same_subject( x->subject, x->len ) )
          return true;
    }
    return false;
  }
};

/* ---- message -------------------------------------------------------------- */

struct api_Msg {
  ommEvent      event;
  const void  * cl;
  uint32_t      refs;         /* detach count; 0 = freed after dispatch */
  bool          in_queue;
  char        * subject;
  uint16_t      subject_len;
  uint8_t       msg_class,    /* ommMsgClass */
                stream_state,
                data_state,
                status_code;
  char        * status_text;
  bool          refresh_complete,
                solicited,
                has_seq,
                sass_done;
  uint32_t      seq_num;
  int64_t       recv_ns;
  uint8_t     * rwf;
  uint32_t      rwf_len;
  MDDict      * dict;
  uint8_t     * sass;
  uint32_t      sass_len;
  uint16_t      msg_type,     /* sass header */
                rec_status;
  omm_status    sass_status;
  MDMsgMem      mem;

  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }
  api_Msg( ommEvent ev,  const void *c )
    : event( ev ), cl( c ), refs( 0 ), in_queue( true ), subject( 0 ),
      subject_len( 0 ), msg_class( 0 ), stream_state( OMM_STREAM_UNSPECIFIED ),
      data_state( OMM_DATA_NO_CHANGE ), status_code( 0 ), status_text( 0 ),
      refresh_complete( false ), solicited( false ), has_seq( false ),
      sass_done( false ), seq_num( 0 ), recv_ns( 0 ), rwf( 0 ), rwf_len( 0 ),
      dict( 0 ), sass( 0 ), sass_len( 0 ), msg_type( 0 ), rec_status( 0 ),
      sass_status( OMM_OK ) {}

  void set_subject( const char *s,  size_t l ) {
    this->subject = (char *) this->mem.make( l + 1 );
    ::memcpy( this->subject, s, l );
    this->subject[ l ] = '\0';
    this->subject_len = (uint16_t) l;
  }
  void set_text( const char *s,  size_t l ) {
    this->status_text = (char *) this->mem.make( l + 1 );
    ::memcpy( this->status_text, s, l );
    this->status_text[ l ] = '\0';
  }
  /* a stream message: copy the rwf bytes, decode the header */
  static api_Msg *make( api_Listener &l,  RwfMsg &m,  MDDict *dict ) noexcept;
  /* a synthesized status (no rwf) */
  static api_Msg *make_status( api_Listener &l,  uint8_t msg_class,
                               uint8_t stream_state,  uint8_t data_state,
                               uint8_t code,  const char *text ) noexcept;
  void sass_header( void ) noexcept;
  omm_status convert_sass( void ) noexcept;
  bool release( void ) noexcept; /* after dispatch; true when freed */
};

/* ---- transport ------------------------------------------------------------ */

struct api_Transport : public EvConnectionNotify, public OmmClientCB,
                       public EvTimerCallback {
  Omm_API       & api;
  ommTransport    id;
  MDMsgDict       dict;
  OmmSourceDB     source_db;
  EvOmmClient     client;
  ListenerHT      ht;
  api_Listener  * state_listener;
  pthread_mutex_t mutex;
  pthread_cond_t  cond;
  char          * daemon, * user, * app_name, * app_id, * password,
                * instance_id, * token, * dict_path, * descr;
  double          reconnect_secs;
  bool            connected,
                  is_destroyed,
                  timer_armed,
                  connect_failed;

  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { aligned_free( ptr ); }
  api_Transport( Omm_API &a,  ommId i ) noexcept;
  ~api_Transport() {
    ApiCore::set_string( this->daemon, NULL );
    ApiCore::set_string( this->user, NULL );
    ApiCore::set_string( this->app_name, NULL );
    ApiCore::set_string( this->app_id, NULL );
    ApiCore::set_string( this->password, NULL );
    ApiCore::set_string( this->instance_id, NULL );
    ApiCore::set_string( this->token, NULL );
    ApiCore::set_string( this->dict_path, NULL );
    ApiCore::set_string( this->descr, NULL );
  }
  void set_params( const ommTransportParams &p ) noexcept;
  EvOmmClientParameters client_params( void ) noexcept {
    return EvOmmClientParameters( this->daemon, this->app_name, this->app_id,
                                  this->user, this->password,
                                  this->instance_id, this->token );
  }
  /* epoll thread */
  bool connect( void ) noexcept;
  void start_stream( api_Listener &l ) noexcept;
  void stop_stream( api_Listener &l ) noexcept;
  void push_status( api_Listener &l,  uint8_t stream_state,
                    uint8_t data_state,  uint8_t code,
                    const char *text ) noexcept;
  void push_state( uint8_t msg_class,  const char *text ) noexcept;
  void arm_reconnect( void ) noexcept;

  virtual void on_connect( EvSocket &conn ) noexcept;
  virtual void on_shutdown( EvSocket &conn,  const char *err,
                            size_t err_len ) noexcept;
  virtual bool on_omm_msg( const char *sub,  size_t sub_len,
                           uint32_t subj_hash,  RwfMsg &msg ) noexcept;
  virtual bool timer_cb( uint64_t timer_id,  uint64_t event_id ) noexcept;
};

/* ---- the api -------------------------------------------------------------- */

struct api_Queue : public ApiQueue {
  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }
  api_Queue( Omm_API &a,  ommId i );
};

struct Omm_API : public ApiCore {
  api_Queue * default_queue;
  void * operator new( size_t, void *ptr ) { return ptr; }
  Omm_API() : ApiCore( 11 ), default_queue( 0 ) {}

  template<class T>
  T *make( ElemType type,  size_t add = 0,  ommId id = 0 ) {
    return this->ApiCore::make<T, Omm_API>( *this, (uint32_t) type, add, id,
                                            type == OMM_ELEM_TRANSPORT );
  }
  virtual void dispatch_event( ApiQueueEvent &ev ) noexcept;

  omm_status open( void ) noexcept;
  api_Transport *get_tport( ommTransport t ) {
    return this->get<api_Transport>( t, OMM_ELEM_TRANSPORT );
  }
  api_Listener *get_listener( ommEvent e ) {
    return this->get<api_Listener>( e, OMM_ELEM_LISTENER );
  }
  /* push a message for listener l on its queue */
  void deliver( api_Listener &l,  api_Msg *m ) noexcept {
    if ( m == NULL )
      return;
    if ( ! this->queue_push( l.queue, l.id, (void *) l.cb, NULL, l.cl, m ) )
      delete m;
  }
};

api_Queue::api_Queue( Omm_API &a,  ommId i ) : ApiQueue( a, i ) {}

static Omm_API * omm_api;
static pthread_mutex_t omm_open_mutex = PTHREAD_MUTEX_INITIALIZER;

static omm_status
api_status( ApiStatus st )
{
  switch ( st ) {
    case API_OK:                  return OMM_OK;
    case API_TIMEOUT:             return OMM_TIMEOUT;
    case API_INVALID_QUEUE:       return OMM_INVALID_QUEUE;
    case API_INVALID_QUEUE_GROUP: return OMM_INVALID_QUEUE_GROUP;
    case API_INVALID_EVENT:       return OMM_INVALID_EVENT;
    case API_INVALID_DISPATCHER:  return OMM_INVALID_DISPATCHER;
    case API_INVALID_ARG:         return OMM_INVALID_ARG;
    default:                      return OMM_INIT_FAILURE;
  }
}

/* ---- transport impl ------------------------------------------------------- */

api_Transport::api_Transport( Omm_API &a,  ommId i ) noexcept
  : api( a ), id( i ), client( a.poll, this->dict, this->source_db ),
    state_listener( 0 ), daemon( 0 ), user( 0 ), app_name( 0 ), app_id( 0 ),
    password( 0 ), instance_id( 0 ), token( 0 ), dict_path( 0 ), descr( 0 ),
    reconnect_secs( DEFAULT_RECONNECT_SECS ), connected( false ),
    is_destroyed( false ), timer_armed( false ), connect_failed( false )
{
  pthread_mutex_init( &this->mutex, NULL );
  pthread_cond_init( &this->cond, NULL );
}

void
api_Transport::set_params( const ommTransportParams &p ) noexcept
{
  ApiCore::set_string( this->daemon, p.daemon != NULL ? p.daemon
                                                      : "127.0.0.1:14002" );
  ApiCore::set_string( this->user, p.user );
  ApiCore::set_string( this->app_name, p.app_name != NULL ? p.app_name
                                                          : "ommapi" );
  ApiCore::set_string( this->app_id, p.app_id != NULL ? p.app_id : "256" );
  ApiCore::set_string( this->password, p.password );
  ApiCore::set_string( this->instance_id, p.instance_id );
  ApiCore::set_string( this->token, p.token );
  ApiCore::set_string( this->dict_path, p.dict_path );
  if ( this->user == NULL ) {
    const char * u = ::getenv( "USER" );
    if ( u == NULL ) u = ::getenv( "LOGNAME" );
    ApiCore::set_string( this->user, u != NULL ? u : "nobody" );
  }
  this->client.no_dictionary = ( p.no_dictionary != 0 );
  if ( ! this->client.no_dictionary && this->dict_path != NULL )
    this->client.have_dictionary = this->dict.load( this->dict_path, false );
  this->reconnect_secs = ( p.reconnect_secs == 0 ? DEFAULT_RECONNECT_SECS
                                                 : p.reconnect_secs );
}

/* epoll thread: (re)connect the client */
bool
api_Transport::connect( void ) noexcept
{
  EvOmmClientParameters parm = this->client_params();
  parm.opts |= kv::OPT_CONNECT_NB;
  if ( ! this->client.omm_connect( parm, this, this ) ) {
    this->connect_failed = true;
    return false;
  }
  return true;
}

void
api_Transport::arm_reconnect( void ) noexcept
{
  if ( this->is_destroyed || this->reconnect_secs < 0 || this->timer_armed )
    return;
  this->timer_armed = true;
  this->api.poll.timer.add_timer_double( *this, this->reconnect_secs,
                                         RECONNECT_TIMER_ID, this->id );
}

bool
api_Transport::timer_cb( uint64_t timer_id,  uint64_t ) noexcept
{
  if ( timer_id != RECONNECT_TIMER_ID )
    return false;
  if ( this->is_destroyed || this->connected ) {
    this->timer_armed = false;
    return false;
  }
  if ( this->client.fd == -1 ) {
    if ( this->connect() )        /* connecting: the timer stays armed until */
      return true;                /* on_connect / on_shutdown decide */
    return true;                  /* failed: retry at the next tick */
  }
  return true;
}

/* login + directory + dictionary done */
void
api_Transport::on_connect( EvSocket & ) noexcept
{
  pthread_mutex_lock( &this->mutex );
  this->connected      = true;
  this->connect_failed = false;
  this->timer_armed    = false;
  this->api.poll.timer.remove_timer_cb( *this, RECONNECT_TIMER_ID, this->id );
  pthread_cond_broadcast( &this->cond );
  /* (re)open every live stream; one request per distinct subject */
  for ( size_t i = 0; this->ht.ht != NULL && i <= this->ht.mask; i++ ) {
    for ( api_Listener *l = this->ht.ht[ i ].hd; l != NULL; l = l->next ) {
      if ( l->done || l->is_state )
        continue;
      bool first = true;
      if ( ( l->flags & OMM_LISTEN_SNAPSHOT ) == 0 ) {
        for ( api_Listener *p = this->ht.ht[ i ].hd; p != l; p = p->next )
          if ( ! p->done && ! p->is_state &&
               ( p->flags & OMM_LISTEN_SNAPSHOT ) == 0 &&
               p->hash == l->hash && p->same_subject( l->subject, l->len ) ) {
            first = false;
            break;
          }
      }
      if ( first )
        this->start_stream( *l );
    }
  }
  this->client.idle_push_write();
  pthread_mutex_unlock( &this->mutex );
  this->push_state( OMM_MSG_TRANSPORT_UP, "connected" );
}

void
api_Transport::on_shutdown( EvSocket &,  const char *err,
                            size_t err_len ) noexcept
{
  char text[ 256 ];
  int  n = ::snprintf( text, sizeof( text ), "transport disconnected%s%.*s",
                       err_len > 0 ? ": " : "", (int) err_len,
                       err != NULL ? err : "" );
  if ( n < 0 || (size_t) n >= sizeof( text ) )
    text[ sizeof( text ) - 1 ] = '\0';
  pthread_mutex_lock( &this->mutex );
  bool was_connected = this->connected;
  this->connected = false;
  pthread_cond_broadcast( &this->cond );
  if ( was_connected ) {
    /* every open stream goes suspect / closed-recover */
    for ( size_t i = 0; this->ht.ht != NULL && i <= this->ht.mask; i++ )
      for ( api_Listener *l = this->ht.ht[ i ].hd; l != NULL; l = l->next )
        if ( ! l->done && ! l->is_state )
          this->push_status( *l, OMM_STREAM_CLOSED_RECOVER, OMM_DATA_SUSPECT,
                             0, text );
  }
  pthread_mutex_unlock( &this->mutex );
  if ( was_connected )
    this->push_state( OMM_MSG_TRANSPORT_DOWN, text );
  this->arm_reconnect();
}

/* epoll thread, mutex held: request the item stream for l's subject */
void
api_Transport::start_stream( api_Listener &l ) noexcept
{
  bool ok;
  if ( ( l.flags & OMM_LISTEN_SNAPSHOT ) != 0 )
    ok = this->client.send_snapshot( l.subject, l.len );
  else
    ok = this->client.send_subscribe( l.subject, l.len,
                                      ( l.flags & OMM_LISTEN_NO_REFRESH ) == 0 );
  if ( ! ok ) {
    /* no directory source for SERVICE.SECTOR: closed, not found */
    l.done = true;
    this->push_status( l, OMM_STREAM_CLOSED, OMM_DATA_SUSPECT,
                       STATUS_CODE_NOT_FOUND, "no source matches subject" );
  }
}

/* epoll thread, mutex held: close the stream when l was its last user;
 * a snapshot stream closes itself with its refresh */
void
api_Transport::stop_stream( api_Listener &l ) noexcept
{
  if ( ( l.flags & OMM_LISTEN_SNAPSHOT ) != 0 )
    return;
  if ( this->connected && ! this->ht.other_live( &l ) ) {
    this->client.send_unsubscribe( l.subject, l.len );
    this->client.idle_push_write();
  }
}

void
api_Transport::push_status( api_Listener &l,  uint8_t stream_state,
                            uint8_t data_state,  uint8_t code,
                            const char *text ) noexcept
{
  l.stream_state = stream_state;
  l.data_state   = data_state;
  this->api.deliver( l, api_Msg::make_status( l, OMM_MSG_STATUS, stream_state,
                                              data_state, code, text ) );
}

void
api_Transport::push_state( uint8_t msg_class,  const char *text ) noexcept
{
  api_Listener * l = this->state_listener;
  if ( l != NULL )
    this->api.deliver( *l, api_Msg::make_status( *l, msg_class,
                              OMM_STREAM_UNSPECIFIED, OMM_DATA_NO_CHANGE, 0,
                              text ) );
}

/* epoll thread: a stream message from the provider.  The stream it came
 * on tells snapshot from subscription: a snapshot's refresh goes to the
 * snapshot listeners, everything on the subscription stream to the
 * streaming ones (the route is still there during the callback) */
bool
api_Transport::on_omm_msg( const char *sub,  size_t sub_len,
                           uint32_t subj_hash,  RwfMsg &m ) noexcept
{
  OmmSubjRoute sub_rt;
  bool is_snap = false;
  if ( this->client.find_stream( m.msg.stream_id, sub_rt, false ) )
    is_snap = ( sub_rt.rt->stream_type == IS_SNAPSHOT );

  pthread_mutex_lock( &this->mutex );
  ListenerList * b = this->ht.bucket( subj_hash );
  if ( b != NULL ) {
    bool closed = false;
    for ( api_Listener *l = b->hd; l != NULL; l = l->next ) {
      if ( l->done || l->is_state || l->hash != subj_hash ||
           ! l->same_subject( sub, sub_len ) )
        continue;
      if ( is_snap != ( ( l->flags & OMM_LISTEN_SNAPSHOT ) != 0 ) )
        continue;
      api_Msg * am = api_Msg::make( *l, m, this->dict.rdm_dict );
      if ( am == NULL )
        continue;
      l->stream_state = am->stream_state;
      l->data_state   = am->data_state;
      bool stream_closed = ( am->stream_state == OMM_STREAM_CLOSED ||
                             am->stream_state == OMM_STREAM_CLOSED_RECOVER ||
                             am->stream_state == OMM_STREAM_REDIRECTED );
      if ( is_snap ) {
        /* the snapshot listener is done with its refresh (the client
         * drops the stream after this callback) */
        if ( ( am->msg_class == OMM_MSG_REFRESH && am->refresh_complete ) ||
             stream_closed )
          l->done = true;
      }
      else if ( stream_closed )
        closed = true;
      this->api.deliver( *l, am );
    }
    if ( closed ) { /* the provider closed it: nothing left to unsubscribe */
      for ( api_Listener *l = b->hd; l != NULL; l = l->next )
        if ( l->hash == subj_hash && ( l->flags & OMM_LISTEN_SNAPSHOT ) == 0 &&
             l->same_subject( sub, sub_len ) )
          l->done = true;
    }
  }
  pthread_mutex_unlock( &this->mutex );
  return true;
}

/* ---- message impl --------------------------------------------------------- */

api_Msg *
api_Msg::make( api_Listener &l,  RwfMsg &m,  MDDict *dict ) noexcept
{
  api_Msg * am = new ( ::malloc( sizeof( api_Msg ) ) ) api_Msg( l.id, l.cl );
  am->set_subject( l.subject, l.len );
  am->recv_ns = current_realtime_ns();
  am->dict    = dict;
  am->rwf_len = (uint32_t) ( m.msg_end - m.msg_off );
  am->rwf     = (uint8_t *) am->mem.make( am->rwf_len );
  ::memcpy( am->rwf, &((uint8_t *) m.msg_buf)[ m.msg_off ], am->rwf_len );

  am->msg_class = m.msg.msg_class;
  am->has_seq   = m.msg.test( X_HAS_SEQ_NUM );
  am->seq_num   = am->has_seq ? m.msg.seq_num : 0;
  if ( m.msg.msg_class == REFRESH_MSG_CLASS ) {
    am->refresh_complete = m.msg.test( X_REFRESH_COMPLETE );
    am->solicited        = m.msg.test( X_SOLICITED );
    am->stream_state     = m.msg.state.stream_state;
    am->data_state       = m.msg.state.data_state;
    am->status_code      = m.msg.state.code;
    if ( m.msg.state.text.len > 0 )
      am->set_text( m.msg.state.text.buf, m.msg.state.text.len );
  }
  else if ( m.msg.msg_class == STATUS_MSG_CLASS ) {
    if ( m.msg.test( X_HAS_STATE ) ) {
      am->stream_state = m.msg.state.stream_state;
      am->data_state   = m.msg.state.data_state;
      am->status_code  = m.msg.state.code;
      if ( m.msg.state.text.len > 0 )
        am->set_text( m.msg.state.text.buf, m.msg.state.text.len );
    }
    else {
      am->stream_state = l.stream_state;
      am->data_state   = l.data_state;
    }
  }
  else { /* update: carry the last known state */
    am->stream_state = l.stream_state;
    am->data_state   = l.data_state;
  }
  am->sass_header();
  return am;
}

api_Msg *
api_Msg::make_status( api_Listener &l,  uint8_t msg_class,
                      uint8_t stream_state,  uint8_t data_state,
                      uint8_t code,  const char *text ) noexcept
{
  api_Msg * am = new ( ::malloc( sizeof( api_Msg ) ) ) api_Msg( l.id, l.cl );
  am->set_subject( l.subject, l.len );
  am->recv_ns      = current_realtime_ns();
  am->msg_class    = msg_class;
  am->stream_state = stream_state;
  am->data_state   = data_state;
  am->status_code  = code;
  if ( text != NULL )
    am->set_text( text, ::strlen( text ) );
  am->sass_header();
  return am;
}

/* rwf class / state -> sass MSG_TYPE / REC_STATUS (rv_cache's mapping) */
void
api_Msg::sass_header( void ) noexcept
{
  this->rec_status = MD_OK_STATUS;
  switch ( this->msg_class ) {
    case OMM_MSG_REFRESH:
      this->msg_type = MD_INITIAL_TYPE;
      break;
    case OMM_MSG_UPDATE:
      this->msg_type = MD_UPDATE_TYPE; /* refined by convert_sass() */
      break;
    case OMM_MSG_STATUS:
    default:
      if ( this->stream_state != OMM_STREAM_OPEN &&
           this->stream_state != OMM_STREAM_UNSPECIFIED )
        this->msg_type = MD_DROP_TYPE;
      else
        this->msg_type = MD_TRANSIENT_TYPE;
      switch ( this->status_code ) {
        case STATUS_CODE_NOT_FOUND: this->rec_status = MD_NOT_FOUND_STATUS; break;
        case STATUS_CODE_TIMEOUT:   this->rec_status = MD_TIMEOUT_STATUS;   break;
        default:
          if ( this->msg_class == OMM_MSG_TRANSPORT_DOWN ||
               ( this->rwf == NULL &&
                 this->stream_state == OMM_STREAM_CLOSED_RECOVER ) )
            this->rec_status = MD_TPT_DISCONNECTED_STATUS;
          else if ( this->data_state == OMM_DATA_SUSPECT )
            this->rec_status = MD_STALE_VALUE_STATUS;
          break;
      }
      break;
  }
}

/* build the sass-form RVMSG from the rwf message */
omm_status
api_Msg::convert_sass( void ) noexcept
{
  if ( this->sass_done )
    return this->sass_status;
  this->sass_done = true;

  MDMsgMem tmp;
  RwfMsg * m      = NULL;
  RwfMsg * fields = NULL;
  if ( this->rwf != NULL ) {
    m = RwfMsg::unpack_message( this->rwf, 0, this->rwf_len, RWF_MSG_TYPE_ID,
                                this->dict, tmp );
    if ( m == NULL ) {
      this->sass_status = OMM_CONVERT_FAILED;
      return this->sass_status;
    }
    fields = m->get_container_msg();
    if ( m->msg.msg_class == UPDATE_MSG_CLASS )
      this->msg_type = rwf_to_sass_msg_type( *m );
    else if ( m->msg.msg_class == STATUS_MSG_CLASS )
      this->rec_status = rwf_code_to_sass_rec_status( *m );
  }
  size_t sz = ( fields != NULL ? ( fields->msg_end - fields->msg_off ) : 0 )
              + 1024;
  void * bp = this->mem.make( sz );
  RvMsgWriter w( this->mem, bp, sz );
  w.append_uint( MD_SASS_MSG_TYPE, MD_SASS_MSG_TYPE_LEN, this->msg_type );
  if ( this->has_seq )
    w.append_uint( MD_SASS_SEQ_NO, MD_SASS_SEQ_NO_LEN, this->seq_num );
  w.append_uint( MD_SASS_REC_STATUS, MD_SASS_REC_STATUS_LEN, this->rec_status );
  int status = 0;
  if ( fields != NULL )
    status = w.convert_msg( *fields, true );
  size_t out = w.update_hdr();
  if ( status != 0 || w.err != 0 ) {
    this->sass_status = OMM_CONVERT_FAILED;
    return this->sass_status;
  }
  this->sass     = (uint8_t *) w.buf;
  this->sass_len = (uint32_t) out;
  return OMM_OK;
}

bool
api_Msg::release( void ) noexcept
{
  this->in_queue = false;
  if ( this->refs == 0 ) {
    delete this;
    return true;
  }
  return false;
}

/* ---- dispatch ------------------------------------------------------------- */

void
Omm_API::dispatch_event( ApiQueueEvent &ev ) noexcept
{
  api_Msg * m = (api_Msg *) ev.msg;
  if ( ev.cb == NULL )
    return;
  ( (ommEventCallback) ev.cb )( ev.id, (ommMsg) m, (void *) ev.cl );
  if ( m != NULL )
    m->release();
  else {
    api_Timer * t = this->get_timer( ev.id );
    if ( t != NULL )
      t->in_queue = false;
  }
}

omm_status
Omm_API::open( void ) noexcept
{
  if ( this->open_pipe( 128 ) != API_OK )
    return OMM_INIT_FAILURE;
  if ( this->open_default_pipe( "omm_api_pipe" ) != API_OK )
    return OMM_INIT_FAILURE;
  this->default_queue = this->make<api_Queue>( OMM_ELEM_QUEUE, 0,
                                               OMM_DEFAULT_QUEUE );
  this->start_ev_thread();
  return OMM_OK;
}

/* ---- epoll thread ops ----------------------------------------------------- */

struct TportOp : public ApiOp {
  api_Transport & t;
  int             what; /* 0 connect, 1 close */
  TportOp( api_Transport &tp,  int w )
    : ApiOp( &tp.mutex, &tp.cond ), t( tp ), what( w ) {}
  virtual void run( ApiPipe & ) noexcept {
    if ( this->what == 0 )
      this->t.connect();
    else {
      this->t.is_destroyed = true;
      this->t.api.poll.timer.remove_timer_cb( this->t, RECONNECT_TIMER_ID,
                                              this->t.id );
      if ( this->t.client.fd != -1 )
        this->t.client.idle_push( EV_CLOSE );
    }
  }
};

struct ListenOp : public ApiOp {
  api_Transport & t;
  api_Listener  & l;
  int             what; /* 0 start, 1 stop */
  ListenOp( api_Transport &tp,  api_Listener &ln,  int w )
    : ApiOp( &tp.mutex, &tp.cond ), t( tp ), l( ln ), what( w ) {}
  virtual void run( ApiPipe & ) noexcept {
    if ( this->what == 0 ) {
      bool first = ( ( this->l.flags & OMM_LISTEN_SNAPSHOT ) != 0 ||
                     ! this->t.ht.other_live( &this->l ) );
      this->t.ht.push( &this->l );
      if ( this->t.connected && first ) {
        this->t.start_stream( this->l );
        this->t.client.idle_push_write();
      }
    }
    else {
      this->t.ht.remove( &this->l );
      if ( ! this->l.done )
        this->t.stop_stream( this->l );
      this->l.done = true;
    }
  }
};

} /* namespace ommapi */

using namespace ommapi;

#define API_CHECK() \
  if ( omm_api == NULL ) return OMM_NOT_INITIALIZED

/* ---- C api ---------------------------------------------------------------- */
extern "C" {

void
omm_InitTransportParams( ommTransportParams *p )
{
  ::memset( p, 0, sizeof( *p ) );
}

omm_status
omm_Open( void )
{
  omm_status st = OMM_OK;
  pthread_mutex_lock( &omm_open_mutex );
  if ( omm_api == NULL ) {
    Omm_API * api = new ( aligned_malloc( sizeof( Omm_API ) ) ) Omm_API();
    st = api->open();
    if ( st == OMM_OK )
      omm_api = api;
  }
  pthread_mutex_unlock( &omm_open_mutex );
  return st;
}

omm_status
omm_Close( void )
{
  return OMM_OK; /* the epoll thread lives for the process, like tibrv */
}

const char *
omm_Version( void )
{
  return STR( OMM_VER );
}

const char *
ommStatus_GetText( omm_status status )
{
  switch ( status ) {
    case OMM_OK:                  return "OK";
    case OMM_INIT_FAILURE:        return "Initialization failed";
    case OMM_NOT_INITIALIZED:     return "omm_Open() not called";
    case OMM_INVALID_TRANSPORT:   return "Invalid transport";
    case OMM_INVALID_QUEUE:       return "Invalid queue";
    case OMM_INVALID_QUEUE_GROUP: return "Invalid queue group";
    case OMM_INVALID_EVENT:       return "Invalid event";
    case OMM_INVALID_DISPATCHER:  return "Invalid dispatcher";
    case OMM_INVALID_ARG:         return "Invalid argument";
    case OMM_INVALID_SUBJECT:     return "Invalid subject, want SERVICE.SECTOR.RIC";
    case OMM_NO_SOURCE:           return "No source matches subject";
    case OMM_NOT_CONNECTED:       return "Transport not connected";
    case OMM_TIMEOUT:             return "Timeout";
    case OMM_NOT_SUPPORTED:       return "Not supported";
    case OMM_NO_MEMORY:           return "Out of memory";
    case OMM_INVALID_MSG:         return "Invalid message";
    case OMM_CONVERT_FAILED:      return "Message conversion failed";
    default:                      return "Unknown status";
  }
}

/* --- transport --- */

omm_status
ommTransport_Create( ommTransport *tport,  const ommTransportParams *parms )
{
  API_CHECK();
  *tport = OMM_INVALID_ID;
  ommTransportParams def;
  if ( parms == NULL ) {
    omm_InitTransportParams( &def );
    parms = &def;
  }
  api_Transport * t = omm_api->make<api_Transport>( OMM_ELEM_TRANSPORT );
  t->set_params( *parms );
  TportOp op( *t, 0 );
  pthread_mutex_lock( &t->mutex );
  omm_api->ev_read->exec( op );
  pthread_mutex_unlock( &t->mutex );
  *tport = t->id;
  return OMM_OK;
}

omm_status
ommTransport_Destroy( ommTransport tport )
{
  API_CHECK();
  api_Transport * t = omm_api->rem<api_Transport>( tport, OMM_ELEM_TRANSPORT );
  if ( t == NULL )
    return OMM_INVALID_TRANSPORT;
  TportOp op( *t, 1 );
  pthread_mutex_lock( &t->mutex );
  omm_api->ev_read->exec( op );
  pthread_mutex_unlock( &t->mutex );
  /* the client socket is released by the epoll thread; the transport
   * record stays (listeners may still reference it) */
  return OMM_OK;
}

omm_status
ommTransport_IsConnected( ommTransport tport,  int *connected )
{
  API_CHECK();
  api_Transport * t = omm_api->get_tport( tport );
  if ( t == NULL )
    return OMM_INVALID_TRANSPORT;
  *connected = t->connected ? 1 : 0;
  return OMM_OK;
}

omm_status
ommTransport_WaitConnected( ommTransport tport,  double timeout )
{
  API_CHECK();
  api_Transport * t = omm_api->get_tport( tport );
  if ( t == NULL )
    return OMM_INVALID_TRANSPORT;
  omm_status st = OMM_OK;
  pthread_mutex_lock( &t->mutex );
  if ( ! t->connected ) {
    struct timespec ts = api_ts_timeout( timeout, 1.0 );
    for (;;) {
      if ( timeout == OMM_WAIT_FOREVER )
        pthread_cond_wait( &t->cond, &t->mutex );
      else if ( pthread_cond_timedwait( &t->cond, &t->mutex, &ts ) == ETIMEDOUT )
        break;
      if ( t->connected || t->is_destroyed )
        break;
    }
    if ( ! t->connected )
      st = ( t->is_destroyed ? OMM_INVALID_TRANSPORT : OMM_TIMEOUT );
  }
  pthread_mutex_unlock( &t->mutex );
  return st;
}

omm_status
ommTransport_HaveDictionary( ommTransport tport,  int *have_dict )
{
  API_CHECK();
  api_Transport * t = omm_api->get_tport( tport );
  if ( t == NULL )
    return OMM_INVALID_TRANSPORT;
  *have_dict = ( t->client.have_dictionary || t->dict.rdm_dict != NULL ) ? 1 : 0;
  return OMM_OK;
}

omm_status
ommTransport_GetDaemon( ommTransport tport,  const char **daemon )
{
  API_CHECK();
  api_Transport * t = omm_api->get_tport( tport );
  if ( t == NULL )
    return OMM_INVALID_TRANSPORT;
  *daemon = t->daemon;
  return OMM_OK;
}

omm_status
ommTransport_GetUser( ommTransport tport,  const char **user )
{
  API_CHECK();
  api_Transport * t = omm_api->get_tport( tport );
  if ( t == NULL )
    return OMM_INVALID_TRANSPORT;
  *user = t->user;
  return OMM_OK;
}

omm_status
ommTransport_SetDescription( ommTransport tport,  const char *d )
{
  API_CHECK();
  api_Transport * t = omm_api->get_tport( tport );
  if ( t == NULL )
    return OMM_INVALID_TRANSPORT;
  ApiCore::set_string( t->descr, d );
  return OMM_OK;
}

omm_status
ommTransport_GetDescription( ommTransport tport,  const char **d )
{
  API_CHECK();
  api_Transport * t = omm_api->get_tport( tport );
  if ( t == NULL )
    return OMM_INVALID_TRANSPORT;
  *d = t->descr;
  return OMM_OK;
}

omm_status
ommTransport_GetSourceCount( ommTransport tport,  uint32_t *count )
{
  API_CHECK();
  api_Transport * t = omm_api->get_tport( tport );
  if ( t == NULL )
    return OMM_INVALID_TRANSPORT;
  uint32_t n = 0;
  pthread_mutex_lock( &t->mutex );
  for ( size_t i = 0; i < t->source_db.source_list.count; i++ )
    for ( OmmSource *s = t->source_db.source_list.ptr[ i ].hd; s != NULL;
          s = s->next )
      n++;
  pthread_mutex_unlock( &t->mutex );
  *count = n;
  return OMM_OK;
}

omm_status
ommTransport_GetSource( ommTransport tport,  uint32_t idx,
                        const char **service_name,  uint32_t *service_id,
                        int *is_up )
{
  API_CHECK();
  api_Transport * t = omm_api->get_tport( tport );
  if ( t == NULL )
    return OMM_INVALID_TRANSPORT;
  omm_status st = OMM_INVALID_ARG;
  uint32_t   n  = 0;
  pthread_mutex_lock( &t->mutex );
  for ( size_t i = 0; i < t->source_db.source_list.count && st != OMM_OK; i++ )
    for ( OmmSource *s = t->source_db.source_list.ptr[ i ].hd; s != NULL;
          s = s->next ) {
      if ( n++ == idx ) {
        *service_name = s->info.service_name;
        *service_id   = s->service_id;
        *is_up        = ( s->state.service_state != 0 );
        st = OMM_OK;
        break;
      }
    }
  pthread_mutex_unlock( &t->mutex );
  return st;
}

omm_status
ommTransport_MatchSubject( ommTransport tport,  const char *subject,
                           const char **service_name,  uint8_t *domain,
                           const char **ric )
{
  API_CHECK();
  api_Transport * t = omm_api->get_tport( tport );
  if ( t == NULL )
    return OMM_INVALID_TRANSPORT;
  const char * r   = subject;
  size_t       len = ::strlen( subject );
  uint8_t      dom = 0;
  pthread_mutex_lock( &t->mutex );
  OmmSource * src = t->source_db.match_sub( r, len, dom, 0 );
  pthread_mutex_unlock( &t->mutex );
  if ( src == NULL )
    return OMM_NO_SOURCE;
  if ( service_name != NULL ) *service_name = src->info.service_name;
  if ( domain != NULL )       *domain = dom;
  if ( ric != NULL )          *ric = r;
  return OMM_OK;
}

omm_status
ommTransport_SetStateListener( ommTransport tport,  ommQueue queue,
                               void ( *cb )( ommEvent, ommMsg, void * ),
                               const void *closure,  ommEvent *event )
{
  API_CHECK();
  *event = OMM_INVALID_ID;
  api_Transport * t = omm_api->get_tport( tport );
  if ( t == NULL )
    return OMM_INVALID_TRANSPORT;
  if ( omm_api->get_queue( queue ) == NULL )
    return OMM_INVALID_QUEUE;
  api_Listener * l = omm_api->make<api_Listener>( OMM_ELEM_LISTENER );
  l->subject  = ::strdup( "_TRANSPORT" );
  l->len      = 10;
  l->hash     = kv_crc_c( l->subject, l->len, 0 );
  l->cb       = cb;
  l->cl       = closure;
  l->queue    = queue;
  l->tport    = tport;
  l->is_state = true;
  pthread_mutex_lock( &t->mutex );
  api_Listener * old = t->state_listener;
  t->state_listener = l;
  pthread_mutex_unlock( &t->mutex );
  if ( old != NULL ) {
    omm_api->rem<api_Listener>( old->id, OMM_ELEM_LISTENER );
    delete old;
  }
  *event = l->id;
  return OMM_OK;
}

/* --- queues --- */

omm_status
ommQueue_Create( ommQueue *queue )
{
  API_CHECK();
  api_Queue * q = omm_api->make<api_Queue>( OMM_ELEM_QUEUE );
  *queue = q->id;
  return OMM_OK;
}
omm_status
ommQueue_Destroy( ommQueue queue )
{
  API_CHECK();
  return api_status( omm_api->destroy_queue( queue, NULL, NULL ) );
}
omm_status
ommQueue_DestroyEx( ommQueue queue,  ommQueueOnComplete cb,
                    const void *closure )
{
  API_CHECK();
  return api_status( omm_api->destroy_queue( queue, (ApiQueueOnComplete) cb,
                                             closure ) );
}
omm_status
ommQueue_TimedDispatch( ommQueue queue,  double timeout )
{
  API_CHECK();
  return api_status( omm_api->timed_dispatch_queue( queue, timeout ) );
}
omm_status
ommQueue_TimedDispatchOneEvent( ommQueue queue,  double timeout )
{
  API_CHECK();
  return api_status( omm_api->timed_dispatch_one_event( queue, timeout ) );
}
omm_status
ommQueue_Dispatch( ommQueue queue )
{
  return ommQueue_TimedDispatch( queue, OMM_WAIT_FOREVER );
}
omm_status
ommQueue_Poll( ommQueue queue )
{
  return ommQueue_TimedDispatch( queue, OMM_NO_WAIT );
}
omm_status
ommQueue_GetCount( ommQueue queue,  uint32_t *num_events )
{
  API_CHECK();
  return api_status( omm_api->get_queue_count( queue, *num_events ) );
}
omm_status
ommQueue_SetPriority( ommQueue queue,  uint32_t priority )
{
  API_CHECK();
  ApiQueue * q = omm_api->get_queue( queue );
  if ( q == NULL || q->done )
    return OMM_INVALID_QUEUE;
  q->priority = priority;
  if ( q->grp != NULL )
    q->grp->update = true;
  return OMM_OK;
}
omm_status
ommQueue_GetPriority( ommQueue queue,  uint32_t *priority )
{
  API_CHECK();
  ApiQueue * q = omm_api->get_queue( queue );
  if ( q == NULL )
    return OMM_INVALID_QUEUE;
  *priority = q->priority;
  return OMM_OK;
}
omm_status
ommQueue_SetName( ommQueue queue,  const char *name )
{
  API_CHECK();
  ApiQueue * q = omm_api->get_queue( queue );
  if ( q == NULL )
    return OMM_INVALID_QUEUE;
  ApiCore::set_string( q->name, name );
  return OMM_OK;
}
omm_status
ommQueue_GetName( ommQueue queue,  const char **name )
{
  API_CHECK();
  ApiQueue * q = omm_api->get_queue( queue );
  if ( q == NULL )
    return OMM_INVALID_QUEUE;
  *name = q->name;
  return OMM_OK;
}

omm_status
ommQueueGroup_Create( ommQueueGroup *grp )
{
  API_CHECK();
  *grp = omm_api->create_queue_group()->id;
  return OMM_OK;
}
omm_status
ommQueueGroup_Destroy( ommQueueGroup grp )
{
  API_CHECK();
  return api_status( omm_api->destroy_queue_group( grp ) );
}
omm_status
ommQueueGroup_Add( ommQueueGroup grp,  ommQueue queue )
{
  API_CHECK();
  return api_status( omm_api->add_queue_group( grp, queue ) );
}
omm_status
ommQueueGroup_Remove( ommQueueGroup grp,  ommQueue queue )
{
  API_CHECK();
  return api_status( omm_api->remove_queue_group( grp, queue ) );
}
omm_status
ommQueueGroup_TimedDispatch( ommQueueGroup grp,  double timeout )
{
  API_CHECK();
  return api_status( omm_api->timed_dispatch_group( grp, timeout ) );
}

omm_status
ommDispatcher_Create( ommDispatcher *disp,  ommDispatchable able,
                      double idle_timeout )
{
  API_CHECK();
  ApiDispatcher * d = omm_api->create_dispatcher( able, idle_timeout );
  *disp = d->id;
  return OMM_OK;
}
omm_status
ommDispatcher_Join( ommDispatcher disp )
{
  API_CHECK();
  return api_status( omm_api->join_dispatcher( disp ) );
}
omm_status
ommDispatcher_Destroy( ommDispatcher disp )
{
  API_CHECK();
  ApiDispatcher * d = omm_api->get_dispatcher( disp );
  if ( d == NULL )
    return OMM_INVALID_DISPATCHER;
  d->quit = true;
  return OMM_OK;
}
omm_status
ommDispatcher_SetName( ommDispatcher disp,  const char *name )
{
  API_CHECK();
  ApiDispatcher * d = omm_api->get_dispatcher( disp );
  if ( d == NULL )
    return OMM_INVALID_DISPATCHER;
  ApiCore::set_string( d->name, name );
  return OMM_OK;
}
omm_status
ommDispatcher_GetName( ommDispatcher disp,  const char **name )
{
  API_CHECK();
  ApiDispatcher * d = omm_api->get_dispatcher( disp );
  if ( d == NULL )
    return OMM_INVALID_DISPATCHER;
  *name = d->name;
  return OMM_OK;
}

/* --- events --- */

omm_status
ommEvent_CreateListener( ommEvent *event,  ommQueue queue,  ommTransport tport,
                         const char *subject,  int flags,  ommEventCallback cb,
                         const void *closure )
{
  API_CHECK();
  *event = OMM_INVALID_ID;
  api_Transport * t = omm_api->get_tport( tport );
  if ( t == NULL || t->is_destroyed )
    return OMM_INVALID_TRANSPORT;
  if ( omm_api->get_queue( queue ) == NULL )
    return OMM_INVALID_QUEUE;
  if ( subject == NULL || cb == NULL )
    return OMM_INVALID_ARG;
  /* SERVICE.SECTOR.RIC needs two dots */
  size_t len = ::strlen( subject );
  const char * d1 = (const char *) ::memchr( subject, '.', len );
  if ( d1 == NULL ||
       ::memchr( d1 + 1, '.', len - ( d1 + 1 - subject ) ) == NULL )
    return OMM_INVALID_SUBJECT;
  if ( ( flags & ( OMM_LISTEN_STREAMING | OMM_LISTEN_SNAPSHOT ) ) == 0 )
    flags |= OMM_LISTEN_STREAMING;

  api_Listener * l = omm_api->make<api_Listener>( OMM_ELEM_LISTENER );
  l->subject = ::strdup( subject );
  l->len     = (uint16_t) len;
  l->hash    = kv_crc_c( subject, len, 0 );
  l->cb      = cb;
  l->cl      = closure;
  l->queue   = queue;
  l->tport   = tport;
  l->flags   = flags;

  ListenOp op( *t, *l, 0 );
  pthread_mutex_lock( &t->mutex );
  omm_api->ev_read->exec( op );
  pthread_mutex_unlock( &t->mutex );
  *event = l->id;
  return OMM_OK;
}

omm_status
ommEvent_CreateTimer( ommEvent *event,  ommQueue queue,  ommEventCallback cb,
                      double interval,  const void *closure )
{
  API_CHECK();
  uint32_t  id;
  ApiStatus st = omm_api->create_timer( id, queue, (void *) cb, interval,
                                        closure );
  *event = ( st == API_OK ? id : OMM_INVALID_ID );
  return api_status( st );
}

omm_status
ommEvent_Destroy( ommEvent event )
{
  API_CHECK();
  ommEventType type;
  omm_status st = ommEvent_GetType( event, &type );
  if ( st != OMM_OK )
    return st;
  switch ( type ) {
    case OMM_TIMER_EVENT: {
      api_Timer * t = omm_api->destroy_timer( event );
      if ( t == NULL )
        return OMM_INVALID_EVENT;
      delete t;
      return OMM_OK;
    }
    case OMM_LISTEN_EVENT:
    case OMM_TRANSPORT_EVENT: {
      api_Listener * l = omm_api->rem<api_Listener>( event, OMM_ELEM_LISTENER );
      if ( l == NULL )
        return OMM_INVALID_EVENT;
      api_Transport * t = omm_api->get_tport( l->tport );
      l->cb = NULL;
      if ( t != NULL ) {
        if ( l->is_state ) {
          pthread_mutex_lock( &t->mutex );
          if ( t->state_listener == l )
            t->state_listener = NULL;
          pthread_mutex_unlock( &t->mutex );
        }
        else {
          ListenOp op( *t, *l, 1 );
          pthread_mutex_lock( &t->mutex );
          omm_api->ev_read->exec( op );
          pthread_mutex_unlock( &t->mutex );
        }
      }
      delete l;
      return OMM_OK;
    }
  }
  return OMM_INVALID_EVENT;
}

omm_status
ommEvent_GetType( ommEvent event,  ommEventType *type )
{
  API_CHECK();
  omm_status st = OMM_INVALID_EVENT;
  pthread_mutex_lock( &omm_api->map_mutex );
  if ( event < omm_api->map_size && omm_api->map[ event ].id == event &&
       omm_api->map[ event ].ptr != NULL ) {
    switch ( omm_api->map[ event ].type ) {
      case OMM_ELEM_TIMER:
        *type = OMM_TIMER_EVENT; st = OMM_OK; break;
      case OMM_ELEM_LISTENER:
        *type = ( ((api_Listener *) omm_api->map[ event ].ptr)->is_state
                  ? OMM_TRANSPORT_EVENT : OMM_LISTEN_EVENT );
        st = OMM_OK; break;
      default: break;
    }
  }
  pthread_mutex_unlock( &omm_api->map_mutex );
  return st;
}

omm_status
ommEvent_GetQueue( ommEvent event,  ommQueue *queue )
{
  API_CHECK();
  *queue = OMM_INVALID_ID;
  pthread_mutex_lock( &omm_api->map_mutex );
  if ( event < omm_api->map_size && omm_api->map[ event ].id == event &&
       omm_api->map[ event ].ptr != NULL ) {
    switch ( omm_api->map[ event ].type ) {
      case OMM_ELEM_TIMER:
        *queue = ((api_Timer *) omm_api->map[ event ].ptr)->queue; break;
      case OMM_ELEM_LISTENER:
        *queue = ((api_Listener *) omm_api->map[ event ].ptr)->queue; break;
      case OMM_ELEM_QUEUE:
        *queue = event; break;
      default: break;
    }
  }
  pthread_mutex_unlock( &omm_api->map_mutex );
  return *queue != OMM_INVALID_ID ? OMM_OK : OMM_INVALID_EVENT;
}

omm_status
ommEvent_GetListenerSubject( ommEvent event,  const char **subject )
{
  API_CHECK();
  api_Listener * l = omm_api->get_listener( event );
  if ( l == NULL )
    return OMM_INVALID_EVENT;
  *subject = l->subject;
  return OMM_OK;
}

omm_status
ommEvent_GetListenerTransport( ommEvent event,  ommTransport *tport )
{
  API_CHECK();
  api_Listener * l = omm_api->get_listener( event );
  if ( l == NULL )
    return OMM_INVALID_EVENT;
  *tport = l->tport;
  return OMM_OK;
}

omm_status
ommEvent_GetTimerInterval( ommEvent event,  double *interval )
{
  API_CHECK();
  return api_status( omm_api->get_timer_interval( event, *interval ) );
}

omm_status
ommEvent_ResetTimerInterval( ommEvent event,  double interval )
{
  API_CHECK();
  return api_status( omm_api->reset_timer_interval( event, interval ) );
}

/* --- messages --- */

#define MSG_CHECK() if ( msg == NULL ) return OMM_INVALID_MSG

omm_status
ommMsg_GetSubject( ommMsg msg,  const char **subject )
{
  MSG_CHECK();
  *subject = ((api_Msg *) msg)->subject;
  return OMM_OK;
}
omm_status
ommMsg_GetEvent( ommMsg msg,  ommEvent *event )
{
  MSG_CHECK();
  *event = ((api_Msg *) msg)->event;
  return OMM_OK;
}
omm_status
ommMsg_GetMsgClass( ommMsg msg,  ommMsgClass *msg_class )
{
  MSG_CHECK();
  *msg_class = (ommMsgClass) ((api_Msg *) msg)->msg_class;
  return OMM_OK;
}
omm_status
ommMsg_GetState( ommMsg msg,  ommStreamState *stream_state,
                 ommDataState *data_state,  uint8_t *status_code )
{
  MSG_CHECK();
  api_Msg * m = (api_Msg *) msg;
  if ( stream_state != NULL ) *stream_state = (ommStreamState) m->stream_state;
  if ( data_state != NULL )   *data_state = (ommDataState) m->data_state;
  if ( status_code != NULL )  *status_code = m->status_code;
  return OMM_OK;
}
omm_status
ommMsg_GetStatusText( ommMsg msg,  const char **text )
{
  MSG_CHECK();
  api_Msg * m = (api_Msg *) msg;
  *text = ( m->status_text != NULL ? m->status_text : "" );
  return OMM_OK;
}
omm_status
ommMsg_IsRefreshComplete( ommMsg msg,  int *complete )
{
  MSG_CHECK();
  *complete = ((api_Msg *) msg)->refresh_complete ? 1 : 0;
  return OMM_OK;
}
omm_status
ommMsg_IsSolicited( ommMsg msg,  int *solicited )
{
  MSG_CHECK();
  *solicited = ((api_Msg *) msg)->solicited ? 1 : 0;
  return OMM_OK;
}
omm_status
ommMsg_GetSeqNum( ommMsg msg,  int *has_seq,  uint32_t *seq_num )
{
  MSG_CHECK();
  api_Msg * m = (api_Msg *) msg;
  if ( has_seq != NULL ) *has_seq = m->has_seq ? 1 : 0;
  if ( seq_num != NULL ) *seq_num = m->seq_num;
  return OMM_OK;
}
omm_status
ommMsg_GetRecvTime( ommMsg msg,  int64_t *recv_ns )
{
  MSG_CHECK();
  *recv_ns = ((api_Msg *) msg)->recv_ns;
  return OMM_OK;
}
omm_status
ommMsg_GetRwf( ommMsg msg,  const void **buf,  uint32_t *len )
{
  MSG_CHECK();
  api_Msg * m = (api_Msg *) msg;
  *buf = m->rwf;
  *len = m->rwf_len;
  return OMM_OK;
}
omm_status
ommMsg_GetSassMsg( ommMsg msg,  const void **buf,  uint32_t *len )
{
  MSG_CHECK();
  api_Msg * m = (api_Msg *) msg;
  omm_status st = m->convert_sass();
  *buf = m->sass;
  *len = m->sass_len;
  return st;
}
omm_status
ommMsg_GetSassHeader( ommMsg msg,  uint16_t *msg_type,  uint16_t *rec_status )
{
  MSG_CHECK();
  api_Msg * m = (api_Msg *) msg;
  if ( m->msg_class == OMM_MSG_UPDATE && ! m->sass_done )
    m->convert_sass(); /* the update kind needs the field list */
  if ( msg_type != NULL )   *msg_type = m->msg_type;
  if ( rec_status != NULL ) *rec_status = m->rec_status;
  return OMM_OK;
}
omm_status
ommMsg_Detach( ommMsg msg )
{
  MSG_CHECK();
  __sync_fetch_and_add( &((api_Msg *) msg)->refs, 1 );
  return OMM_OK;
}
omm_status
ommMsg_Destroy( ommMsg msg )
{
  MSG_CHECK();
  api_Msg * m = (api_Msg *) msg;
  if ( m->refs == 0 )
    return OMM_INVALID_MSG;
  if ( __sync_sub_and_fetch( &m->refs, 1 ) == 0 && ! m->in_queue )
    delete m;
  return OMM_OK;
}
omm_status
ommMsg_Print( ommMsg msg,  void *fp )
{
  MSG_CHECK();
  api_Msg * m = (api_Msg *) msg;
  FILE    * f = ( fp != NULL ? (FILE *) fp : stdout );
  fprintf( f, "%s: class=%u stream=%u data=%u code=%u%s%s\n", m->subject,
           m->msg_class, m->stream_state, m->data_state, m->status_code,
           m->status_text != NULL ? " text=" : "",
           m->status_text != NULL ? m->status_text : "" );
  if ( m->rwf == NULL )
    return OMM_OK;
  MDMsgMem tmp;
  RwfMsg * rm = RwfMsg::unpack_message( m->rwf, 0, m->rwf_len,
                                        RWF_MSG_TYPE_ID, m->dict, tmp );
  if ( rm == NULL )
    return OMM_CONVERT_FAILED;
  MDOutput out;
  out.filep = f; /* print() writes through filep, stdout when NULL */
  rm->print( &out );
  out.filep = NULL;
  return OMM_OK;
}

} /* extern "C" */
