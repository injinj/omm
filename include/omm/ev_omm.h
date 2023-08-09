#ifndef __rai_omm__ev_omm_h__
#define __rai_omm__ev_omm_h__

#include <raikv/ev_tcp.h>
#include <raikv/route_ht.h>
#include <raikv/dlinklist.h>
#include <raikv/array_space.h>
#include <raikv/route_ht.h>
#include <raimd/rwf_msg.h>
#include <raimd/md_dict.h>
#include <omm/ipc.h>

namespace rai {
namespace omm {

#define is_omm_debug kv_unlikely( omm_debug != 0 )
extern uint32_t omm_debug;
struct LoginInfo;
struct OmmSourceDB;
struct OmmSource;
struct DictInProg;
struct IpcFrag;

struct OmmDict {
  md::MDDict * dict,
             * flist_dict,
             * rdm_dict,
             * cfile_dict;
  OmmDict() : dict( 0 ), flist_dict( 0 ), rdm_dict( 0 ), cfile_dict( 0 ) {}
  bool load_cfiles( const char *cfile_path ) noexcept;
};

struct EvOmmListen : public kv::EvTcpListen {
  kv::RoutePublish & sub_route;
  OmmDict          & dict;
  OmmSourceDB      & x_source_db;
  EvOmmListen( kv::EvPoll &p,  OmmDict &d,  OmmSourceDB &db ) noexcept;
  EvOmmListen( kv::EvPoll &p,  OmmDict &d,  OmmSourceDB &db,
               kv::RoutePublish &sr ) noexcept;
  virtual kv::EvSocket *accept( void ) noexcept;
  void add_source( md::RwfMsg &map ) noexcept;
};

struct OmmRoute {
  uint32_t stream_id,
           service_id,
           hash,
           msg_cnt;
  uint8_t  domain;
  bool     is_solicited;
  uint16_t len;
  char     value[ 2 ];
};

struct OmmSubTab : public kv::RouteVec<OmmRoute> {};

struct OmmSubjRoute {
  OmmRoute   * rt;
  kv::RouteLoc loc;
  size_t       pos;
  uint32_t     hash,
               hcnt;
};

struct OmmSubject {
  OmmSource * src;
  char      * sub;
  size_t      sub_len;
  uint32_t    hash;
};

struct TempBuf {
  uint8_t * msg; /* pre allocated on connect stream buf */
  size_t    len;
};

struct EvOmmConn : public kv::EvConnection {
  kv::RoutePublish & sub_route;
  IpcFrag            ipc_fragment;
  size_t             max_frag_size;
  uint32_t           next_frag_num,
                     src_count;
  OmmSubTab          sub_tab;
  kv::UIntHashTab  * stream_ht;
  OmmSourceDB      & source_db;
  OmmDict          & dict;

  EvOmmConn( kv::EvPoll &p,  uint8_t st,  kv::RoutePublish &sr,  OmmDict &d,
             OmmSourceDB &db )
    : kv::EvConnection( p, st ), sub_route( sr ), max_frag_size( 6 * 1024 ),
      next_frag_num( 0 ), src_count( 0 ), stream_ht( 0 ), source_db( db ),
      dict( d ) {}

  void init_streams( void ) noexcept;
  void release_streams( void ) noexcept;
  TempBuf mktemp( size_t sz ) {
    if ( sz > this->max_frag_size - 3 )
      sz = this->max_frag_size - 3;
    uint8_t * p = (uint8_t *) this->alloc_temp( sz );
    return { p + 3, sz - 3 };
  }
  void send_msg( const char *what,  md::RwfMsgWriter &msg,
                 TempBuf &temp_buf ) noexcept;
  void fragment_msg( const uint8_t *buf,  const size_t len,
                     const uint32_t stream_id ) noexcept;
  static void debug_print( const char *what,  md::RwfMsg &msg ) noexcept;
  static void debug_print( const char *what,  md::RwfMsgWriter &msg ) noexcept;
  static bool rejected( const char *what,  md::RwfMsg &msg ) noexcept;

  bool find_stream( uint32_t stream_id,  OmmSubjRoute &subj,
                    bool check_coll ) noexcept;
  bool msg_key_to_sub( md::RwfMsgHdr &hdr,  OmmSubject &subj ) noexcept;
  bool add_subj_stream( md::RwfMsgHdr &hdr,  OmmSubject &subj,
                        OmmSubjRoute &sub_rt ) noexcept;
  void publish_msg( md::RwfMsg &msg,  OmmSubjRoute &sub_rt ) noexcept;
  void close_streams( void ) noexcept;
  virtual bool on_msg( kv::EvPublish &pub ) noexcept;
};

struct EvOmmService : public EvOmmConn {
  EvOmmListen & listener;
  LoginInfo   * login;

  void * operator new( size_t, void *ptr ) { return ptr; }
  EvOmmService( kv::EvPoll &p,  uint8_t st,  EvOmmListen &l )
    : EvOmmConn( p, st, l.sub_route, l.dict, l.x_source_db ),
      listener( l ), login( 0 ) {}
  virtual bool timer_expire( uint64_t timer_id, uint64_t event_id ) noexcept;
  virtual void process( void ) noexcept;
  virtual void release( void ) noexcept;
  virtual void process_shutdown( void ) noexcept;
  virtual void process_close( void ) noexcept;
  bool dispatch_msg( IpcHdr &ipc,  char *buf ) noexcept;
  void recv_login_request( md::RwfMsg &msg ) noexcept;
  md::RwfElementListWriter &
    add_login_response_attrs( md::RwfElementListWriter &elist,
                              LoginInfo *info ) noexcept;
  md::RwfMapWriter &
    add_source_dirs( md::RwfMapWriter &map, uint32_t filter ) noexcept;
  void recv_directory_request( md::RwfMsg &msg ) noexcept;
  void recv_dictionary_request( md::RwfMsg &msg ) noexcept;
  void process_msg( md::RwfMsg &msg ) noexcept;
  void send_status( md::RwfMsg &msg,  uint8_t status_code,
                    const char *descr = NULL ) noexcept;
};

}
}

#endif
