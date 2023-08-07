#ifndef __rai_omm__rv_submgr_h__
#define __rai_omm__rv_submgr_h__

#include <omm/ev_omm.h>
#include <sassrv/ev_rv_client.h>
#include <sassrv/submgr.h>

namespace rai {
namespace omm {

struct ReplyEntry {
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
  static bool equals( const ReplyEntry &e,  const void *s,  uint16_t l ) {
    return e.sublen == l && ::memcmp( s, e.value, l ) == 0;
  }
};

struct ReplyTab : public kv::RouteVec<ReplyEntry, nullptr, ReplyEntry::equals> {};


/* rv client callback closure */
struct RvOmmSubmgr : public kv::EvSocket, public kv::EvConnectionNotify,
                     public sassrv::RvClientCB,
                     public sassrv::SubscriptionListener {
  sassrv::EvRvClient   & client;          /* connection to rv */
  sassrv::SubscriptionDB sub_db;
  OmmDict              & dict;
  ReplyTab               reply_tab;
  const char          ** sub;             /* subject strings */
  size_t                 sub_count;       /* count of sub[] */
  bool                   is_subscribed;   /* sub[] are subscribed */

  void * operator new( size_t, void *ptr ) { return ptr; }
  RvOmmSubmgr( kv::EvPoll &p,  sassrv::EvRvClient &c,
               OmmDict &d ) noexcept;
  bool convert_to_msg( kv::EvPublish &pub,  uint32_t type_id ) noexcept;
  /* after CONNECTED message */
  virtual void on_connect( kv::EvSocket &conn ) noexcept;
  /* start sub[] with inbox reply */
  void start_subscriptions( void ) noexcept;
  /* when signalled, unsubscribe */
  void on_unsubscribe( void ) noexcept;
  /* when disconnected */
  virtual void on_shutdown( kv::EvSocket &conn,  const char *err,
                            size_t err_len ) noexcept;
  /* RvClientCB */
  virtual bool on_rv_msg( kv::EvPublish &pub ) noexcept;
  /* SubscriptionListener  */
  virtual void on_listen_start( sassrv::StartListener &add ) noexcept;
  virtual void on_listen_stop ( sassrv::StopListener  &rem ) noexcept;
  virtual void on_snapshot    ( sassrv::SnapListener  &snp ) noexcept;
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
