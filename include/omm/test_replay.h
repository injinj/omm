#ifndef __rai_omm__test_replay_h__
#define __rai_omm__test_replay_h__

#include <omm/ev_omm.h>

namespace rai {
using namespace md;
using namespace kv;
namespace omm {

struct TestReplay : public EvTimerCallback {
  EvPoll      & poll;
  OmmDict     & dict;
  OmmSourceDB & source_db;
  EvOmmListen & omm_sv;
  FILE        * fp;
  char        * fn,
              * buf,
              * feed;
  size_t        buflen,
                msg_count,
                feed_len;

  void * operator new( size_t, void *ptr ) { return ptr; }
  TestReplay( EvOmmListen *sv )
    : poll( sv->poll ), dict( sv->dict ), source_db( sv->x_source_db ),
      omm_sv( *sv ), fp( 0 ), fn( 0 ), buf( 0 ), feed( 0 ), buflen( 0 ),
      msg_count( 0 ), feed_len( 0 ) {}

  void add_replay_file( const char *feed_name,  uint32_t service_id,
                        const char *replay_file ) noexcept;
  void start( void ) noexcept;
  virtual bool timer_cb( uint64_t timer_id,  uint64_t event_id ) noexcept;
};

}
}

#endif
