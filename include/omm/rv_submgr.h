#ifndef __rai_omm__rv_submgr_h__
#define __rai_omm__rv_submgr_h__

#include <omm/ev_omm.h>
#include <sassrv/ev_rv_client.h>
#include <sassrv/submgr.h>
#include <sassrv/ft.h>

namespace rai {
namespace omm {

static const int MAX_FMT_PREFIX = 4;

struct InboxReplyEntry { /* inbox replies for refresh requests */
  uint32_t hash;
  uint16_t sublen;
  uint16_t len;
  char     value[ 4 ];

  bool first_reply( size_t &pos, const char *&reply,  size_t &reply_len ) {
    pos = this->sublen;
    return this->next_reply( pos, reply, reply_len );
  }
  bool next_reply( size_t &pos, const char *&reply,  size_t &reply_len ) {
    if ( pos + 1 >= this->len )
      return false;
    reply     = &this->value[ pos + 1 ];
    reply_len = ::strlen( reply );
    pos      += 1 + reply_len;
    return true;
  }
  static bool equals( const InboxReplyEntry &e,  const void *s,  uint16_t l ) {
    return e.sublen == l && ::memcmp( s, e.value, l ) == 0;
  }
};
/* map subject -> inbox reply list */
struct InboxReplyTab : public kv::RouteVec< InboxReplyEntry, nullptr,
                                            InboxReplyEntry::equals > {};

struct WildEntry {
  uint32_t hash,
           refcnt;
  uint16_t len;
  char     value[ 2 ];

  void init( void ) {
    this->refcnt = 0;
  }
};
/* wildcard subscriptions */
struct WildTab : public kv::RouteVec< WildEntry > {};

struct FlistEntry {
  md::MDFormClass * form;
  uint32_t flist    : 16,
           rec_type : 15,
           bcast    : 1;
  uint32_t hash;

  FlistEntry() : form( 0 ), flist( 0 ), rec_type( 0 ), bcast( 0 ), hash( 0 ) {}
  FlistEntry( uint32_t h )
    : form( 0 ), flist( 0 ), rec_type( 0 ), bcast( 0 ), hash( h ) {}
  FlistEntry( const FlistEntry &fe )
    : form( fe.form ), flist( fe.flist ), rec_type( fe.rec_type ),
      bcast( fe.bcast ), hash( fe.hash ) {}
  FlistEntry& operator=( const FlistEntry &fe ) {
    this->form     = fe.form;
    this->flist    = fe.flist;
    this->rec_type = fe.rec_type;
    this->bcast    = fe.bcast;
    this->hash     = fe.hash;
    return *this;
  }
};
/* map of subject_id -> flist */
typedef kv::IntHashTabT< uint32_t, FlistEntry > ActiveHT;

struct Insub {
  sassrv::RvSubscription & script;
  const char * sub;
  size_t       sublen;
  Insub( sassrv::RvSubscription &s,  size_t pref_len  )
    : script( s ), sub( &s.value[ pref_len ] ), sublen( s.len - pref_len ) {}
  uint32_t hash( void ) const noexcept;
};

struct Outsub {
  char * value;
  size_t len;
  char   buf[ 256 ];

  void set( const char *pref,  size_t pref_len,  const char *s,  size_t l,
            md::MDMsgMem &mem ) {
    char * p = this->buf;
    if ( pref_len + l >= sizeof( this->value ) )
      p = mem.str_make( pref_len + l + 1 );
    ::memcpy( p, pref, pref_len );
    ::memcpy( &p[ pref_len ], s, l );
    p[ pref_len + l ] = '\0';
    this->value = p;
    this->len   = pref_len + l;
  }
  uint32_t hash( void ) const noexcept;
};

/* rv client callback closure */
struct RvOmmSubmgr : public kv::EvSocket, public kv::EvConnectionNotify,
                     public sassrv::RvClientCB,
                     public sassrv::RvSubscriptionListener,
                     public sassrv::RvFtListener {
  sassrv::EvRvClient     & client;          /* connection to rv */
  kv::RoutePublish       & sub_route;
  sassrv::RvSubscriptionDB sub_db;
  sassrv::RvFt             ft;
  OmmDict                & dict;
  InboxReplyTab            reply_tab;
  WildTab                  wild_tab;
  kv::UIntHashTab        * coll_ht;
  ActiveHT               * active_ht;
  md::MDMsgMem             cvt_mem;
  uint32_t                 fmt,
                           update_fmt,
                           pref_len,
                           ft_rank;
  bool                     is_stopped,
                           is_running,
                           is_finished;
  sassrv::FtParameters     ft_param;
  md::MDOutput             dbg_out;
  uint64_t                 tid;
  char                     pref[ 16 ];
  const char            ** feed_wild;       /* subject strings */
  size_t                   feed_wild_count; /* count of sub[] */

  static const uint32_t PROCESS_EVENTS_SECS = 2,
                        SYNC_JOIN_MS = 1,
                        PROCESS_EVENTS = 1;

  void * operator new( size_t, void *ptr ) { return ptr; }
  RvOmmSubmgr( kv::EvPoll &p,  sassrv::EvRvClient &c,
               OmmDict &d,  uint32_t ft_weight,
               const char *prefix,  const char *msg_fmt ) noexcept;
  void update_field_list( FlistEntry &flist,  uint16_t flist_no ) noexcept;
  int convert_to_msg( kv::EvPublish &pub,  uint32_t type_id,
                      FlistEntry &flist,  bool &flist_updated ) noexcept;
  /* after CONNECTED message */
  virtual void on_connect( kv::EvSocket &conn ) noexcept;
  /* start sub[] with inbox reply */
  void on_start( void ) noexcept;
  /* when signalled, unsubscribe */
  void on_stop( void ) noexcept;
  uint32_t sub_refcnt( Insub &m ) noexcept;
  void add_collision( uint32_t h ) noexcept;
  bool rem_collision( uint32_t h ) noexcept;
  /* when disconnected */
  virtual void on_shutdown( kv::EvSocket &conn,  const char *err,
                            size_t err_len ) noexcept;
  /* RvClientCB */
  virtual bool on_rv_msg( kv::EvPublish &pub ) noexcept;
  /* RvFtListener */
  virtual void on_ft_change( uint8_t action ) noexcept;
  virtual void on_ft_sync( kv::EvPublish &pub ) noexcept;
  void activate_subs( void ) noexcept;
  void start_sub( sassrv::RvSubscription &sub,  bool is_bcast_reply,
                  bool is_ft_activate,  const void *reply,
                  size_t reply_len ) noexcept;
  void stop_sub( sassrv::RvSubscription &sub,  bool is_ft_deactivate ) noexcept;
  void deactivate_subs( void ) noexcept;
  /* SubscriptionListener  */
  virtual void on_listen_start( Start &add ) noexcept;
  virtual void on_listen_stop ( Stop  &rem ) noexcept;
  virtual void on_snapshot    ( Snap  &snp ) noexcept;
  /* EvSocket */
  virtual bool on_msg( kv::EvPublish &pub ) noexcept;
  virtual bool timer_expire( uint64_t tid,  uint64_t eid ) noexcept;
  virtual void write( void ) noexcept;
  virtual void read( void ) noexcept;
  virtual void process( void ) noexcept;
  virtual void release( void ) noexcept;
  virtual void on_write_ready( void ) noexcept;
};


}
}

#endif
