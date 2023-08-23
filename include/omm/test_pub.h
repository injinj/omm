#ifndef __rai_omm__test_pub_h__
#define __rai_omm__test_pub_h__

#include <omm/ev_omm.h>

namespace rai {
using namespace md;
using namespace kv;
namespace omm {

struct TestRoute {
  uint64_t  seqno;
  MDDecimal ask,
            bid,
            ask_size,
            bid_size;
  MDTime    time;
  MDDate    date;
  bool      is_active;
  uint32_t  hash;
  uint16_t  len;
  char      value[ 2 ];

  void init( uint64_t cur_ns ) noexcept;
  void update_time( uint64_t cur_ns ) noexcept;
  void update( uint64_t cur_ns ) noexcept;
};

struct TestPublish : public EvSocket, public RouteNotify {
  EvPoll        & poll;
  RoutePublish  & sub_route;
  OmmDict       & dict;
  OmmSourceDB   & source_db;

  RouteVec<TestRoute> test_tab;
  
  void * operator new( size_t, void *ptr ) { return ptr; }
  TestPublish( EvPoll &p,  OmmDict &d,  OmmSourceDB &db ) noexcept;

  void add_test_source( const char *feed_name,  uint32_t service_id ) noexcept;
  void start( void ) noexcept;
  /* Routenotify */
  virtual void on_sub( kv::NotifySub &sub ) noexcept;
  virtual void on_resub( kv::NotifySub &sub ) noexcept;
  virtual void on_unsub( kv::NotifySub &sub ) noexcept;
  virtual void on_psub( kv::NotifyPattern &pat ) noexcept;
  virtual void on_punsub( kv::NotifyPattern &pat ) noexcept;
  void initial( const char *reply,  size_t reply_len,  OmmSource *src,
                const char *ric,  size_t ric_len,  uint8_t domain,
                TestRoute *rt,  bool is_solicited ) noexcept;
  void update( OmmSource *src,  const char *ric,  size_t ric_len,
               uint8_t domain,  TestRoute *rt ) noexcept;
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
