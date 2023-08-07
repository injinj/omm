#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <time.h>
#if ! defined( _MSC_VER ) && ! defined( __MINGW32__ )
#include <unistd.h>
#else
#include <raikv/win.h>
#endif
#include <omm/rv_submgr.h>
#include <omm/src_dir.h>
#include <raikv/ev_publish.h>
#include <raimd/md_msg.h>
#include <raimd/tib_msg.h>
#include <raimd/tib_sass_msg.h>

using namespace rai;
using namespace kv;
using namespace sassrv;
using namespace omm;
using namespace md;

RvOmmSubmgr::RvOmmSubmgr( kv::EvPoll &p,  sassrv::EvRvClient &c,
                          OmmDict &d ) noexcept
  : EvSocket( p, p.register_type( "omm_submgr" ) ),
    client( c ), sub_db( c, this ), dict( d ), sub( 0 ), sub_count( 0 ),
    is_subscribed( false )
{
  c.fwd_all_msgs = 0;
  this->sock_opts = OPT_NO_POLL;
}

/* called after daemon responds with CONNECTED message */
void
RvOmmSubmgr::on_connect( EvSocket &conn ) noexcept
{
  int len = (int) conn.get_peer_address_strlen();
  printf( "Connected: %.*s\n", len, conn.peer_address.buf );
  fflush( stdout );
  for ( size_t i = 0; i < this->sub_count; i++ )
    this->sub_db.add_wildcard( this->sub[ i ] );
  this->start_subscriptions();
}

/* start subscriptions from command line, inbox number indexes the sub[] */
void
RvOmmSubmgr::start_subscriptions( void ) noexcept
{
  int sfd = this->poll.get_null_fd();
  this->PeerData::init_peer( this->poll.get_next_id(), sfd, -1, NULL,
                             "omm_submgr" );
  this->PeerData::set_name( "omm_submgr", 10 );
  this->poll.add_sock( this );
  this->sub_db.start_subscriptions( this->sub_count == 0 );
  this->poll.timer.add_timer_seconds( this->fd, 3, 1, 0 );
}

void
RvOmmSubmgr::on_unsubscribe( void ) noexcept
{
  this->sub_db.stop_subscriptions();
}

bool
RvOmmSubmgr::on_rv_msg( kv::EvPublish &pub ) noexcept
{
  this->sub_db.process_pub( pub );
  return true;
}

static bool
is_rwf_solicited( EvPublish &pub ) noexcept
{
  if ( pub.msg_enc != RWF_MSG_TYPE_ID )
    return false;
  uint8_t msg_class = RwfMsgPeek::get_msg_class( pub.msg, pub.msg_len );
  if ( msg_class != REFRESH_MSG_CLASS )
    return false;
  uint16_t msg_flags = RwfMsgPeek::get_msg_flags( pub.msg, pub.msg_len );
  return ( msg_flags & RWF_REFRESH_SOLICITED ) != 0;
}

bool
RvOmmSubmgr::convert_to_msg( EvPublish &pub,  uint32_t type_id ) noexcept
{
  if ( this->dict.rdm_dict == NULL || pub.msg_enc != RWF_MSG_TYPE_ID )
    return false;

  MDMsgMem mem;
  RwfMsg * m = RwfMsg::unpack_message( (void *) pub.msg, 0, pub.msg_len,
                                       RWF_MSG_TYPE_ID, this->dict.rdm_dict,
                                       mem );
  if ( m == NULL )
    return false;

  RwfMsg * cmsg = m->get_container_msg();
  if ( cmsg == NULL )
    return false;

  if ( type_id == RVMSG_TYPE_ID ) {
    RvMsgWriter rvmsg( this->client.alloc_temp( 1024 ), 1024 );
    if ( rvmsg.convert_msg( *cmsg, false ) != 0 )
      return false;
    pub.msg     = rvmsg.buf;
    pub.msg_len = rvmsg.update_hdr();
    pub.msg_enc = RVMSG_TYPE_ID;
    return true;
  }
  if ( type_id == TIBMSG_TYPE_ID ) {
    TibMsgWriter tibmsg( this->client.alloc_temp( 1024 ), 1024 );
    if ( tibmsg.convert_msg( *cmsg, false ) != 0 )
      return false;
    pub.msg     = tibmsg.buf;
    pub.msg_len = tibmsg.update_hdr();
    pub.msg_enc = TIBMSG_TYPE_ID;
    return true;
  }
  if ( type_id == TIB_SASS_TYPE_ID ) {
    TibSassMsgWriter sassmsg( this->dict.cfile_dict,
                              this->client.alloc_temp( 1024 ), 1024 );
    if ( sassmsg.convert_msg( *cmsg, false ) != 0 )
      return false;
    pub.msg     = sassmsg.buf;
    pub.msg_len = sassmsg.update_hdr();
    pub.msg_enc = TIB_SASS_TYPE_ID;
    return true;
  }
  return false;
}

bool
RvOmmSubmgr::on_msg( EvPublish &pub ) noexcept
{
  EvPublish pub2( pub );
  if ( ! this->convert_to_msg( pub2, TIB_SASS_TYPE_ID ) ) {
    fprintf( stderr, "failed to convert msg %.*s\n",
             (int) pub.subject_len, pub.subject );
    return true;
  }

  if ( is_rwf_solicited( pub ) ) {
    const char * reply;
    size_t       reply_len;
    RouteLoc     loc;
    size_t       pos;
    ReplyEntry * entry =
      this->reply_tab.find( pub.subj_hash, pub.subject, pub.subject_len,
                            loc );
    if ( entry != NULL ) {
      for ( bool b = entry->first_reply( pos, reply, reply_len ); b;
            b = entry->next_reply( pos, reply, reply_len ) ) {
        pub2.subject     = reply;
        pub2.subject_len = reply_len;
        pub2.subj_hash   = 0;
        printf( "on_initial %.*s\n", (int) pub.subject_len, pub.subject );
        this->client.on_msg( pub2 );
      }
      this->reply_tab.remove( loc );
    }
    return true;
  }
  printf( "on_msg %.*s\n", (int) pub.subject_len, pub.subject );
  this->client.on_msg( pub2 );
  return true;
}

/* timer expired, process rv sub_db events */
bool
RvOmmSubmgr::timer_expire( uint64_t ,  uint64_t ) noexcept
{
  this->sub_db.process_events();
  return true; /* return false to disable recurrent timer */
}
void RvOmmSubmgr::write( void ) noexcept {}
void RvOmmSubmgr::read( void ) noexcept {}
void RvOmmSubmgr::process( void ) noexcept {}
void RvOmmSubmgr::release( void ) noexcept {}
void RvOmmSubmgr::on_write_ready( void ) noexcept {}

void
RvOmmSubmgr::on_listen_start( StartListener &add ) noexcept
{
  if ( add.reply_len == 0 ) {
    printf( "%sstart %.*s refs %u from %.*s\n",
      add.is_listen_start ? "listen_" : "assert_",
      add.sub.len, add.sub.value, add.sub.refcnt,
      add.session.len, add.session.value );
  }
  else {
    printf( "%sstart %.*s reply %.*s refs %u from %.*s\n",
      add.is_listen_start ? "listen_" : "assert_",
      add.sub.len, add.sub.value, add.reply_len, add.reply, add.sub.refcnt,
      add.session.len, add.session.value );
  }
  uint32_t h = kv_crc_c( add.sub.value, add.sub.len, 0 );
  NotifySub nsub( add.sub.value, add.sub.len, NULL, 0, h, 0, 'V', *this );
  RouteLoc loc;

  if ( add.reply_len > 0 ) {
    ReplyEntry * entry =
      this->reply_tab.upsert( h, add.sub.value, (size_t) add.sub.len + 1, loc );
    if ( loc.is_new ) {
      entry->sublen = add.sub.len;
      entry->value[ add.sub.len ] = '\0';
    }
    size_t off = entry->len;
    entry =
      this->reply_tab.resize( h, entry, off, off + add.reply_len + 1, loc );
    ::memcpy( &entry->value[ off ], add.reply, add.reply_len );
    entry->value[ off + add.reply_len ] = '\0';

    nsub.notify_type = NOTIFY_IS_INITIAL;
  }
  else {
    /* in case where snap arrives before subscribe */
    if ( this->reply_tab.find( h, add.sub.value, (size_t) add.sub.len + 1,
                               loc ) != NULL )
      nsub.notify_type = NOTIFY_IS_INITIAL;
  }
  if ( add.sub.refcnt == 1 ) {
    this->client.sub_route.add_sub( nsub );
  }
  else {
    nsub.sub_count = add.sub.refcnt;
    this->client.sub_route.notify_sub( nsub );
  }
}

void
RvOmmSubmgr::on_listen_stop( StopListener &rem ) noexcept
{
  printf( "%sstop %.*s refs %u from %.*s%s\n",
    rem.is_listen_stop ? "listen_" : "assert_",
    rem.sub.len, rem.sub.value, rem.sub.refcnt,
    rem.session.len, rem.session.value, rem.is_orphan ? " orphan" : "" );

  uint32_t h = kv_crc_c( rem.sub.value, rem.sub.len, 0 );
  NotifySub nsub( rem.sub.value, rem.sub.len, NULL, 0, h, 0, 'V', *this );
  if ( rem.sub.refcnt == 0 ) {
    this->reply_tab.remove( h, rem.sub.value, rem.sub.len );
    this->client.sub_route.del_sub( nsub );
  }
  else {
    nsub.sub_count = rem.sub.refcnt;
    this->client.sub_route.notify_unsub( nsub );
  }
}

void
RvOmmSubmgr::on_snapshot( SnapListener &snp ) noexcept
{
  printf( "snap %.*s reply %.*s refs %u flags %u\n",
    snp.sub.len, snp.sub.value, snp.reply_len, snp.reply, snp.sub.refcnt,
    snp.flags );

  if ( snp.reply_len == 0 )
    return;
  uint32_t h = kv_crc_c( snp.sub.value, snp.sub.len, 0 );

  RouteLoc loc;
  ReplyEntry * entry =
    this->reply_tab.upsert( h, snp.sub.value, (size_t) snp.sub.len + 1, loc );
  if ( loc.is_new ) {
    entry->sublen = snp.sub.len;
    entry->value[ snp.sub.len ] = '\0';
  }
  size_t off = entry->len;
  entry =
    this->reply_tab.resize( h, entry, off, off + snp.reply_len + 1, loc );
  ::memcpy( &entry->value[ off ], snp.reply, snp.reply_len );
  entry->value[ off + snp.reply_len ] = '\0';

  if ( snp.sub.refcnt != 0 ) {
    NotifySub nsub( snp.sub.value, snp.sub.len, NULL, 0, h, 0, 'V', *this );
    nsub.notify_type = NOTIFY_IS_INITIAL;
    nsub.sub_count = snp.sub.refcnt;
    this->client.sub_route.notify_sub( nsub );
  }
}

/* when client connection stops */
void
RvOmmSubmgr::on_shutdown( EvSocket &conn,  const char *err,
                          size_t errlen ) noexcept
{
  int len = (int) conn.get_peer_address_strlen();
  printf( "Shutdown: %.*s %.*s\n",
          len, conn.peer_address.buf, (int) errlen, err );
  /* if disconnected by tcp, usually a reconnect protocol, but this just exits*/
  if ( this->poll.quit == 0 )
    this->poll.quit = 1; /* causes poll loop to exit */
}

