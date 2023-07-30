#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <raimd/md_msg.h>
#include <raimd/md_dict.h>
#include <raimd/cfile.h>
#include <raimd/app_a.h>
#include <raimd/enum_def.h>
#include <omm/ev_omm_client.h>
#include <omm/src_dir.h>

using namespace rai;
using namespace kv;
using namespace md;
using namespace omm;

EvOmmClient::EvOmmClient( EvPoll &p,  OmmDict &d,  OmmSourceDB &db ) noexcept
  : EvOmmConn( p, p.register_type( "ommclient" ), p.sub_route, d, db ),
    RouteNotify( p.sub_route ), cb( 0 ), login( 0 ),
    dict_in_progress( 0 ), next_stream_id( 10 ),
    no_dictionary( false ), have_dictionary( false ),
    app_name( 0 ), app_id( 0 ), user( 0 ), pass( 0 ),
    instance_id( 0 ), token( 0 )
{
}
bool OmmClientCB::on_msg( const char *,  size_t ,  uint32_t , 
                          md::RwfMsg & ) noexcept { return true; }

bool
EvOmmClient::connect( EvOmmClientParameters &p,  EvConnectionNotify *n,
                      OmmClientCB *c ) noexcept
{
  char * daemon = NULL, buf[ 256 ];
  int port = p.port;

  if ( this->fd != -1 )
    return false;

  if ( p.daemon != NULL ) {
    size_t len = ::strlen( p.daemon );
    if ( len >= sizeof( buf ) )
      len = sizeof( buf ) - 1;
    ::memcpy( buf, p.daemon, len );
    buf[ len ] = '\0';
    daemon = buf;
  }
  if ( daemon != NULL ) {
    char * pt;
    if ( (pt = ::strrchr( daemon, ':' )) != NULL ) {
      port = atoi( pt + 1 );
      *pt = '\0';
    }
    else {
      for ( pt = daemon; *pt != '\0'; pt++ )
        if ( *pt < '0' || *pt > '9' )
          break;
      if ( *pt == '\0' ) {
        port = atoi( daemon );
        daemon = NULL;
      }
    }
    if ( daemon != NULL ) { /* strip tcp: prefix */
      if ( ::strncmp( daemon, "tcp:", 4 ) == 0 )
        daemon += 4;
      if ( ::strcmp( daemon, "tcp" ) == 0 )
        daemon += 3;
      if ( daemon[ 0 ] == '\0' )
        daemon = NULL;
    }
  }
  if ( EvTcpConnection::connect( *this, daemon, port, p.opts ) != 0 )
    return false;
  this->init_streams();
  this->notify      = n;
  this->cb          = c;
  this->app_name    = p.app_name;
  this->app_id      = p.app_id;
  this->user        = p.user;
  this->pass        = p.pass;
  this->instance_id = p.instance_id;
  this->token       = p.token;
  this->send_client_init_rec();

  return true;
}

void
EvOmmClient::send_client_init_rec( void ) noexcept
{
  ClientInitRec r;
  PeerAddrStr paddr;

  paddr.set_sock_ip( this->fd );
  r.ip_addr_len = paddr.len(),
  ::memcpy( r.ip_addr, paddr.buf, r.ip_addr_len );
  init_component_string( r );

  if ( ::gethostname( r.host, sizeof( r.host ) ) != 0 )
    ::strcpy( r.host, "localhost" );
  r.host_len = ::strlen( r.host );

  size_t len = r.pack_len();
  char * p = this->alloc( len );
  r.pack( p );
  this->sz += len;
  this->idle_push( EV_WRITE );
}

void
EvOmmClient::process( void ) noexcept
{
  bool failed = false;

  while ( this->off < this->len ) {
    size_t buflen = this->len - this->off;
    char * buf = &this->recv[ this->off ];
    IpcHdr ipc;
    int status = ipc.parse( (uint8_t *) buf, buflen );

    if ( status >= 0 ) {
      this->off += ipc.ipc_len;
      if ( ! this->dispatch_msg( ipc, buf ) ) {
        failed = true;
        break;
      }
    }
    else {
      if ( status < IpcHdr::NOT_ENOUGH_DATA ) {
        MDOutput mout;
        printf( "failed: status %d\n", status );
        mout.print_hex( buf, buflen );
        failed = true;
      }
      break;
    }
  }
  this->pop( EV_PROCESS );
  if ( ! failed ) {
    if ( ! this->push_write() )
      this->clear_write_buffers();
    return;
  }
  this->push( EV_CLOSE );
}

bool
EvOmmClient::dispatch_msg( IpcHdr &ipc,  char *buf ) noexcept
{
  /* mormal messages are:
   * [ header ] [ data : next_off - header_len ]
   * msgs can be packed, the for loop advances header_len to next pack len:
   * [ header ] [ pack len ] [ data ] [ pack len ] [ data ]
   */
  for (;;) {
    char * msg = &buf[ ipc.header_len ];
    size_t len = ipc.next_off - ipc.header_len;
    ipc.header_len = ipc.next_off;

    while ( len == 0 ) { /* ping msgs are zero length */
      if ( ipc.next_off + 2 >= ipc.ipc_len )
        return true;
      size_t pack_len = ( (size_t) ((uint8_t *) buf)[ ipc.next_off ] << 8 ) |
                          (size_t) ((uint8_t *) buf)[ ipc.next_off + 1 ];
      ipc.header_len += 2;
      ipc.next_off = ipc.header_len + pack_len;
      if ( ipc.next_off > ipc.ipc_len ) /* should error */
        return true;
      msg = &buf[ ipc.header_len ];
      len = ipc.next_off - ipc.header_len;
      ipc.header_len = ipc.next_off;
    }

    if ( is_omm_debug ) {
      MDOutput mout;
      printf( "message:\n" );
      mout.print_hex( msg, len );
    }

    if ( ipc.frag_id != 0 && len != ipc.extended_len ) {
      if ( ! this->ipc_fragment.merge( ipc.frag_id, ipc.extended_len,
                                       msg, len ) )
        continue;
    }

    if ( ipc.is_conn_ack() ) {
      ServerInitRec rec;
      if ( ! rec.unpack( buf, ipc.ipc_len ) )
        return false;
      if ( is_omm_debug )
        printf( "connack, rssl_flags %u ping_timeout %u max_msg_size %u\n",
          rec.rssl_flags, rec.ping_timeout, rec.max_msg_size );
      if ( rec.max_msg_size > 64 ) {
        this->max_frag_size = rec.max_msg_size;
        if ( this->max_frag_size > 0xffff - 10 )
          this->max_frag_size = 0xffff - 10;
      }
      static const uint64_t PING_TIMER_ID = 1,
                            PING_TIMER_EVENT = 1;
      this->poll.timer.add_timer_millis( this->fd, rec.ping_timeout * 1000 / 2,
                                         PING_TIMER_ID, PING_TIMER_EVENT );
      this->send_login_request();
      continue;
    }

    if ( ipc.is_data() ) {
      MDMsgMem mem;
      RwfMsg * m = RwfMsg::unpack_message( msg, 0, len, RWF_MSG_TYPE_ID,
                                           this->dict.rdm_dict, mem );
      if ( m == NULL )
        return false;

      switch ( m->msg.stream_id ) {
        case login_stream_id:
          this->recv_login_response( *m );
          this->send_directory_request();
          break;
        case dictionary_stream_id:
        case enumdefs_stream_id:
          this->recv_dictionary_response( *m );
          if ( 0 ) {
        case directory_stream_id:
            this->recv_directory_response( *m );
            if ( ! this->have_dictionary && ! this->no_dictionary )
              this->send_dictionary_request();
          }
          if ( this->dict_in_progress == NULL ) {
            if ( this->notify != NULL )
              this->notify->on_connect( *this );
            this->EvOmmConn::sub_route.add_route_notify( *this );
          }
          break;
        default:
          this->forward_msg( *m );
          break;
      }
    }
    /* otherwise, what is ipc? */
  }
}

void
EvOmmClient::subscribe( const char *sub,  size_t len ) noexcept
{
  if ( ! this->send_subscribe( sub, len ) ) {
    fprintf( stderr, "no source matches %.*s\n", (int) len, sub );
  }
}

void
EvOmmClient::unsubscribe( const char *sub,  size_t len ) noexcept
{
  if ( ! this->send_unsubscribe( sub, len ) ) {
    fprintf( stderr, "no source matches %.*s\n", (int) len, sub );
  }
}

void
EvOmmClient::process_shutdown( void ) noexcept
{
  if ( is_omm_debug )
    printf( "shutdown %.*s\n", (int) this->get_peer_address_strlen(),
            this->peer_address.buf );
  this->pushpop( EV_CLOSE, EV_SHUTDOWN );
}

void
EvOmmClient::release( void ) noexcept
{
  if ( is_omm_debug )
    printf( "release %.*s\n", (int) this->get_peer_address_strlen(),
            this->peer_address.buf );
  this->EvConnection::release_buffers();
}

void
EvOmmClient::process_close( void ) noexcept
{
  printf( "close %.*s\n", (int) this->get_peer_address_strlen(),
          this->peer_address.buf );
  if ( this->poll.quit == 0 )
    this->poll.quit = 1;
  this->EvSocket::process_close();
}

bool
EvOmmClient::timer_expire( uint64_t, uint64_t ) noexcept
{
  /* send ping */
  static const uint8_t ping[ 3 ] = { 0, 3, IPC_DATA };
  this->append( ping, sizeof( ping ) );
  this->idle_push( EV_WRITE );
  return true;
}

