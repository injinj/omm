#ifndef __rai_omm__test_replay_h__
#define __rai_omm__test_replay_h__

#include <omm/ev_omm.h>

namespace rai {
using namespace md;
using namespace kv;
namespace omm {

struct TestReplay : public EvSocket {
  EvPoll        & poll;
  RoutePublish  & sub_route;
  OmmDict       & dict;
  OmmSourceDB   & source_db;
  FILE          * fp;
  char          * fn,
                * buf,
                * feed;
  size_t          buflen,
                  msg_count,
                  feed_len;

  void * operator new( size_t, void *ptr ) { return ptr; }
  TestReplay( EvPoll &p,  OmmDict &d,  OmmSourceDB &db ) noexcept;

  void add_replay_file( const char *feed_name,  uint32_t service_id,
                        const char *replay_file ) noexcept;
  void start( void ) noexcept;

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
