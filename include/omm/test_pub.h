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

struct TestPublish : public RouteNotify, public EvTimerCallback {
  EvPoll      & poll;
  OmmDict     & dict;
  OmmSourceDB & source_db;
  EvOmmListen & omm_sv;

  RouteVec<TestRoute> test_tab;
  
  void * operator new( size_t, void *ptr ) { return ptr; }
  TestPublish( EvOmmListen *sv )
    : RouteNotify( sv->poll.sub_route ), poll( sv->poll ),
      dict( sv->dict ), source_db( sv->x_source_db ), omm_sv( *sv ) {}

  void add_test_source( const char *feed_name,  uint32_t service_id ) noexcept;
  void start( void ) noexcept;
  virtual void on_sub( kv::NotifySub &sub ) noexcept;
  virtual void on_resub( kv::NotifySub &sub ) noexcept;
  virtual void on_unsub( kv::NotifySub &sub ) noexcept;
  virtual void on_psub( kv::NotifyPattern &pat ) noexcept;
  virtual void on_punsub( kv::NotifyPattern &pat ) noexcept;
  virtual bool timer_cb( uint64_t timer_id,  uint64_t event_id ) noexcept;
  void initial( const char *reply,  size_t reply_len,  OmmSource *src,
                const char *ric,  size_t ric_len,  uint8_t domain,
                TestRoute *rt,  bool is_solicited ) noexcept;
  void update( OmmSource *src,  const char *ric,  size_t ric_len,
               uint8_t domain,  TestRoute *rt ) noexcept;
};

}
}

#endif
