#ifndef __rai_omm__ev_omm_client_h__
#define __rai_omm__ev_omm_client_h__

#include <omm/ev_omm.h>

namespace rai {
namespace omm {

struct EvOmmClientParameters {
  const char * daemon,
             * app_name,
             * app_id,
             * user,
             * pass,
             * instance_id,
             * token; 
  int          port,
               opts;
  EvOmmClientParameters( const char *d = NULL,  const char *n = NULL,
                         const char *i = NULL,  const char *u = NULL,
                         const char *pwd = NULL,  const char *inst = NULL,
                         const char *tok = NULL,
                         int p = 14002, int o = kv::DEFAULT_TCP_CONNECT_OPTS )
    : daemon( d ), app_name( n ), app_id( i ), user( u ), pass( pwd ),
      instance_id( inst ), token( tok ), port( p ), opts( o ) {}
};

struct EvOmmClient;
struct OmmClientCB {
  OmmClientCB() {}
  virtual bool on_omm_msg( const char *sub,  size_t sub_len,  uint32_t subj_hash,
                           md::RwfMsg &msg ) noexcept;
};

struct LoginInfo;
struct OmmSourceDB;
struct DictInProg;
struct IpcFrag;
#if 0
struct ReplyElem {
  ReplyElem * next;
  uint16_t    reply_len;
  char        reply[ 6 ];
  static ReplyElem * make_reply_elem( const char *reply,
                                      uint16_t reply_len ) noexcept;
};
typedef kv::SLinkList<ReplyElem> ReplyList;
typedef kv::IntHashTabT<uint32_t, ReplyList> ReplyHT;
#endif
struct EvOmmClient : public EvOmmConn, public kv::RouteNotify {
  static const uint32_t login_stream_id      = 1,
                        directory_stream_id  = 2,
                        dictionary_stream_id = 3,
                        enumdefs_stream_id   = 4;
  OmmClientCB * cb;
  LoginInfo   * login;
  DictInProg  * dict_in_progress;
  uint32_t      next_stream_id;
  bool          no_dictionary,   /* don't request dictionary */
                have_dictionary; /* set when dict request succeeded */
  const char  * app_name,        /* connect parameters */
              * app_id,
              * user,
              * pass,
              * instance_id,
              * token;

  void * operator new( size_t, void *ptr ) { return ptr; }
  EvOmmClient( kv::EvPoll &p,  OmmDict &d,  OmmSourceDB &db ) noexcept;
  bool connect( EvOmmClientParameters &p,  kv::EvConnectionNotify *n = NULL,
                OmmClientCB *c = NULL ) noexcept;
  virtual bool timer_expire( uint64_t timer_id, uint64_t event_id ) noexcept;
  virtual void process( void ) noexcept;
  virtual void release( void ) noexcept;
  virtual void process_shutdown( void ) noexcept;
  virtual void process_close( void ) noexcept;

  bool dispatch_msg( IpcHdr &ipc,  char *buf ) noexcept;
  void subscribe( const char *sub,  size_t len ) noexcept;
  void unsubscribe( const char *sub,  size_t len ) noexcept;

  void send_client_init_rec( void ) noexcept;
  void send_login_request( void ) noexcept;
  md::RwfElementListWriter &
    add_login_request_attrs( md::RwfElementListWriter &elist ) noexcept;
  void send_directory_request( void ) noexcept;
  void send_dictionary_request( void ) noexcept;

  bool recv_login_response( md::RwfMsg &msg ) noexcept;
  void recv_directory_response( md::RwfMsg &msg ) noexcept;
  void recv_dictionary_response( md::RwfMsg &msg ) noexcept;
  bool send_subscribe( const char *sub,  size_t len, bool is_initial ) noexcept;
  bool send_snapshot( const char *sub,  size_t len ) noexcept;
  bool send_unsubscribe( const char *sub,  size_t len ) noexcept;
  void forward_msg( md::RwfMsg &msg ) noexcept;

  virtual void on_sub( kv::NotifySub &sub ) noexcept;
  virtual void on_resub( kv::NotifySub &sub ) noexcept;
  virtual void on_unsub( kv::NotifySub &sub ) noexcept;
  virtual void on_psub( kv::NotifyPattern &pat ) noexcept;
  virtual void on_punsub( kv::NotifyPattern &pat ) noexcept;
};

}
}
#endif
