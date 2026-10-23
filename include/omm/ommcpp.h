#ifndef __rai__omm__ommcpp_h__
#define __rai__omm__ommcpp_h__
    
#include <omm/ommapi.h>
#include <raikv/ev_api_queue.h>

using namespace rai;
using namespace md;
using namespace kv;
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

enum { OMM_LISTEN_START = 0, OMM_LISTEN_STOP = 1 };
struct ListenOp : public ApiOp {
  api_Transport & t;
  api_Listener  & l;
  int             what; /* 0 start, 1 stop */
  ListenOp( api_Transport &tp,  api_Listener &ln,  int w )
    : ApiOp( &tp.mutex, &tp.cond ), t( tp ), l( ln ), what( w ) {}
  virtual void run( ApiPipe & ) noexcept {
    if ( this->what == OMM_LISTEN_START ) {
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

enum { OMM_TPORT_CONNECT = 0, OMM_TPORT_CLOSE = 1 };
struct TportOp : public ApiOp {
  api_Transport & t;
  int             what; /* 0 connect, 1 close */
  TportOp( api_Transport &tp,  int w )
    : ApiOp( &tp.mutex, &tp.cond ), t( tp ), what( w ) {}
  virtual void run( ApiPipe & ) noexcept {
    if ( this->what == OMM_TPORT_CONNECT )
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

} /* namespace ommapi */

#endif
