#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#if ! defined( _MSC_VER ) && ! defined( __MINGW32__ )
#include <arpa/inet.h>
#else
#include <raikv/win.h>
#endif
#include <omm/ev_omm.h>
#include <omm/ipc.h>
#include <raikv/key_hash.h>
#include <raikv/util.h>
#include <raikv/ev_publish.h>
#include <raikv/timer_queue.h>
#define PCRE2_CODE_UNIT_WIDTH 8
#include <pcre2.h>
#include <raikv/pattern_cvt.h>
#include <raimd/json_msg.h>
#include <raimd/tib_msg.h>
#include <raimd/tib_sass_msg.h>
#include <raimd/rwf_msg.h>
#include <raimd/mf_msg.h>

using namespace rai;
using namespace kv;
using namespace omm;
using namespace md;

extern "C" {
const char *
omm_get_version( void )
{
  return kv_stringify( OMM_VER );
}
}
uint32_t rai::omm::omm_debug = 0;

EvOmmListen::EvOmmListen( EvPoll &p,  OmmDict &d ) noexcept
           : EvTcpListen( p, "omm_listen", "omm_sock" ),
             sub_route( p.sub_route ), dict( d ), x_source_db( 0 )
{
  this->init();
}

EvOmmListen::EvOmmListen( EvPoll &p,  OmmDict &d,  RoutePublish &sr ) noexcept
           : EvTcpListen( p, "omm_listen", "omm_sock" ),
             sub_route( sr ), dict( d ), x_source_db( 0 )
{
  this->init();
}

EvSocket *
EvOmmListen::accept( void ) noexcept
{
  EvOmmService *c =
    this->poll.get_free_list<EvOmmService, EvOmmListen &>(
      this->accept_sock_type, *this );
  if ( c == NULL )
    return NULL;
  if ( this->accept2( *c, "omm" ) ) {
    printf( "accept %.*s\n", (int) c->get_peer_address_strlen(),
            c->peer_address.buf );
    c->init( this->x_source_db );
    return c;
  }
  return NULL;
}

void
EvOmmService::process( void ) noexcept
{
  bool failed = false;
  while ( this->off < this->len ) {
    size_t buflen = this->len - this->off;
    char * buf = &this->recv[ this->off ];
    IpcHdr ipc;
    int status = ipc.parse( (uint8_t *) buf, buflen );

    if ( status >= 0 ) {
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
EvOmmService::dispatch_msg( IpcHdr &ipc,  char *buf ) noexcept
{
  char * msg = &buf[ ipc.header_len ];
  size_t len = ipc.ipc_len - ipc.header_len;
  MDOutput mout;

  if ( len == 0 ) /* ping msg */
    return true;
  if ( is_omm_debug ) {
    ipc.print( buf );
    printf( "message:\n" );
    mout.print_hex( msg, len );
  }
  if ( ipc.frag_id != 0 && len != ipc.extended_len ) {
    if ( ! this->ipc_fragment.merge( ipc.frag_id, ipc.extended_len, msg, len ) )
      return true;
  }
  if ( ipc.is_conn_init() ) {
    ClientInitRec c;
    if ( ! c.unpack( buf, ipc.ipc_len ) )
      return false;
    static const uint64_t PING_TIMER_ID = 1,
                          PING_TIMER_EVENT = 1;
    this->poll.timer.add_timer_millis( this->fd, c.ping_timeout * 1000 / 2,
                                       PING_TIMER_ID, PING_TIMER_EVENT );
    ServerInitRec r;
    init_component_string( r );
    size_t len = r.pack_len();
    char * p = this->alloc( len );
    r.pack( p );
    this->sz += len; 
  }
  else if ( ipc.is_data() ) {
    MDMsgMem mem;
    RwfMsg * m = RwfMsg::unpack_message( msg, 0, len, RWF_MSG_TYPE_ID,
                                         this->dict.rdm_dict, mem );
    if ( m == NULL )
      return false;
    if ( m->msg.domain_type >= RDM_DOMAIN_COUNT ) {
      m->print( &mout );
      this->send_status( *m, STATUS_CODE_USAGE_ERROR,
                         "Domain type not supported" );
    }
    printf( "%s domain\n", rdm_domain_str[ m->msg.domain_type ] );
    switch ( m->msg.domain_type ) {
      case LOGIN_DOMAIN:
        this->recv_login_request( *m );
        break;
      case SOURCE_DOMAIN:
        this->recv_directory_request( *m );
        break;
      case DICTIONARY_DOMAIN:
        this->recv_dictionary_request( *m );
        break;
      default:
        this->process_msg( *m );
        break;
    }
  }
  return true;
}

void
EvOmmService::release( void ) noexcept
{
  printf( "release %.*s\n", (int) this->get_peer_address_strlen(),
          this->peer_address.buf );
  this->EvConnection::release_buffers();
}

void
EvOmmService::process_shutdown( void ) noexcept
{
  printf( "shutdown %.*s\n", (int) this->get_peer_address_strlen(),
          this->peer_address.buf );
  this->pushpop( EV_CLOSE, EV_SHUTDOWN );
}

void
EvOmmService::process_close( void ) noexcept
{
  printf( "close %.*s\n", (int) this->get_peer_address_strlen(),
          this->peer_address.buf );
  this->EvSocket::process_close();
}

bool
EvOmmService::timer_expire( uint64_t, uint64_t ) noexcept
{
  /* send ping */
  static const uint8_t ping[ 3 ] = { 0, 3, IPC_DATA };
  this->append( ping, sizeof( ping ) );
  this->idle_push( EV_WRITE );
  return true;
}

void
IpcHdr::print( void *buf ) noexcept
{
  printf( "len %u hdr %u ext_len %x next_off %u frag %u op_code %u ext_op %u\n",
    (uint32_t) this->ipc_len, (uint32_t) this->header_len,
    (uint32_t) this->extended_len, (uint32_t) this->next_off,
    this->frag_id, this->op_code, this->extended_op );
  if ( this->op_code == IPC_INIT_CLIENT )  printf( "IPC_INIT_CLIENT " );
  if ( ( this->op_code & IPC_DATA ) != 0 ) printf( "IPC_DATA " );
  if ( ( this->op_code & IPC_EXTENDED_FLAGS ) != 0 ) {
    printf( "IPC_EXT " );
    if ( ( this->extended_op & EXTENDED_IPC_CONNACK ) != 0 ) printf( "CONN_ACK " );
    if ( ( this->extended_op & EXTENDED_IPC_CONNNAK ) != 0 ) printf( "CONN_NAK " );
    if ( ( this->extended_op & EXTENDED_IPC_FRAG ) != 0 ) printf( "CONN_FRAG " );
    if ( ( this->extended_op & EXTENDED_IPC_FRAG_HEADER ) != 0 ) printf( "CONN_FRAG_HEADER " );
  }
  if ( ( this->op_code & IPC_DATA ) != 0 ) printf( "IPC_DATA " );
  printf( "\n" );
  MDOutput mout;
  mout.print_hex( buf, this->header_len );
  printf( "----\n" );
}

void
EvOmmConn::debug_print( const char *what,  RwfMsg &msg ) noexcept
{
  printf( "-- %s:\n", what );
  MDOutput mout;
  msg.print( &mout );
}

void
EvOmmConn::debug_print( const char *what,  RwfMsgWriter &msg ) noexcept
{
  printf( "-- %s:\n", what );
  MDOutput mout;
  MDMsgMem mem;
  MDMsg *m = MDMsg::unpack( msg.buf, 0, msg.off, RWF_MSG_TYPE_ID, NULL, mem );
  if ( m != NULL )
    m->print( &mout );
}

bool
EvOmmConn::rejected( const char *what,  RwfMsg &msg ) noexcept
{
  fprintf( stderr, "-- %s rejected:\n", what );
  MDOutput mout;
  msg.print( &mout );
  return false;
}

void
EvOmmConn::send_msg( const char *what,  RwfMsgWriter &msg,
                     TempBuf &temp_buf ) noexcept
{
  if ( msg.err != 0 ) {
    fprintf( stderr, "msg for %s has error %d\n", what, msg.err );
    return;
  }
  if ( is_omm_debug )
    debug_print( what, msg );

  if ( msg.buf < temp_buf.msg || msg.buf >= &temp_buf.msg[ temp_buf.len ] ) {
    this->fragment_msg( msg.buf, msg.off, 0 );
    return;
  }

  uint16_t  len = msg.off + 3;
  uint8_t * buf = msg.buf - 3;

  buf[ 0 ] = (uint8_t) ( ( len >> 8 ) & 0xff );
  buf[ 1 ] = (uint8_t) ( len & 0xff );
  buf[ 2 ] = IPC_DATA;
  this->append_iov( buf, len );
}

bool
IpcFrag::merge( uint16_t cur_frag_id,  uint32_t cur_ext_len,  char *&msg,
                size_t &len ) noexcept
{
  if ( cur_ext_len != 0 ) {
    if ( this->extended_len != 0 ) {
      fprintf( stderr, "unconsumed fragment %u size %u cur_frag %u new_size %u\n",
        this->frag_id, this->extended_len, cur_frag_id, cur_ext_len );
      return false;
    }
    if ( cur_ext_len > this->extended_len )
      this->msg_buf = (char *) ::realloc( this->msg_buf, cur_ext_len );
    this->extended_len = cur_ext_len;
    this->frag_id      = cur_frag_id;
    this->frag_off     = len;
    ::memcpy( this->msg_buf, msg, len );
    return false;
  }
  if ( cur_frag_id == this->frag_id ) {
    if ( this->frag_off + len > this->extended_len ) {
      fprintf( stderr, "fragment %u size %u buffer overrun %u\n",
        this->frag_id, this->extended_len, (uint32_t) ( this->frag_off + len ) );
      this->extended_len = this->frag_off + len;
      this->msg_buf = (char *) ::realloc( this->msg_buf, this->extended_len );
    }
    ::memcpy( &this->msg_buf[ this->frag_off ], msg, len );
    this->frag_off += len;
    if ( this->frag_off == this->extended_len ) {
      this->extended_len = 0;
      this->frag_id      = 0;

      msg = this->msg_buf;
      len = this->frag_off;
      return true;
    }
    return false;
  }
  fprintf( stderr, "unconsumed fragment %u size %u new frag %u len %u\n",
           this->frag_id, this->extended_len, cur_frag_id, (uint32_t) len );
  return false;
}

void
EvOmmConn::fragment_msg( const uint8_t *buf,  const size_t len,
                         const uint32_t stream_id ) noexcept
{
  size_t    first_frag = this->max_frag_size - 10,
            next_frag  = this->max_frag_size - 6,
            frag_cnt   = 1 + ( (len - first_frag + next_frag - 1) / next_frag ),
            frag_len   = first_frag + 10;
  uint8_t * frag       = (uint8_t *) this->alloc( frag_len );
  uint16_t  frag_num   = ++this->next_frag_num;
  if ( frag_num == 0 )
    frag_num = ++this->next_frag_num;

  frag[ 0 ] = ( frag_len >> 8 ) & 0xffU;
  frag[ 1 ] = frag_len & 0xffU;
  frag[ 2 ] = IPC_DATA | IPC_EXTENDED_FLAGS;
  frag[ 3 ] = EXTENDED_IPC_FRAG_HEADER | EXTENDED_IPC_FRAG;
  frag[ 4 ] = ( len >> 24 ) & 0xffU;
  frag[ 5 ] = ( len >> 16 ) & 0xffU;
  frag[ 6 ] = ( len >> 8 ) & 0xffU;
  frag[ 7 ] = len & 0xffU;
  frag[ 8 ] = ( frag_num >> 8 ) & 0xffU;
  frag[ 9 ] = frag_num & 0xffU;
  ::memcpy( &frag[ 10 ], buf, first_frag );
  if ( stream_id != 0 )
    set_u32<MD_BIG>( &frag[ 10 + 4 ], stream_id );
  this->sz += frag_len;
  size_t off = first_frag;

  for ( size_t i = 1; i < frag_cnt - 1; i++ ) {
    frag_len = next_frag + 6;
    frag     = (uint8_t *) this->alloc( frag_len );

    frag[ 0 ] = ( frag_len >> 8 ) & 0xffU;
    frag[ 1 ] = frag_len & 0xffU;
    frag[ 2 ] = IPC_DATA | IPC_EXTENDED_FLAGS;
    frag[ 3 ] = EXTENDED_IPC_FRAG;
    frag[ 4 ] = ( frag_num >> 8 ) & 0xffU;
    frag[ 5 ] = frag_num & 0xffU;
    ::memcpy( &frag[ 6 ], &buf[ off ], next_frag );
    this->sz += frag_len;
    off += next_frag;
  }

  size_t last_frag = len - off;
  frag_len = last_frag + 6;
  frag     = (uint8_t *) this->alloc( frag_len );

  frag[ 0 ] = ( frag_len >> 8 ) & 0xffU;
  frag[ 1 ] = frag_len & 0xffU;
  frag[ 2 ] = IPC_DATA | IPC_EXTENDED_FLAGS;
  frag[ 3 ] = EXTENDED_IPC_FRAG;
  frag[ 4 ] = ( frag_num >> 8 ) & 0xffU;
  frag[ 5 ] = frag_num & 0xffU;
  ::memcpy( &frag[ 6 ], &buf[ off ], last_frag );
  this->sz += frag_len;
}

void
EvOmmService::send_status( RwfMsg &msg,  uint8_t status_code,
                           const char *descr ) noexcept
{
  RwfMsgHdr  & hdr = msg.msg;
  MDMsgMem     mem;
  TempBuf      temp_buf = this->mktemp( 256 );
  RwfMsgWriter resp( mem, NULL, temp_buf.msg, temp_buf.len,
                     STATUS_MSG_CLASS, (RdmDomainType) hdr.domain_type,
                     hdr.stream_id );
  if ( descr == NULL )
    descr = ( status_code < RDM_STATUS_CODE_COUNT ?
              rdm_status_code_str[ status_code ] : "Error" );
  resp.add_state( DATA_STATE_SUSPECT, STREAM_STATE_CLOSED, descr, status_code );

  RwfMsgKey & msg_key = hdr.msg_key;
  if ( msg_key.test( X_HAS_SERVICE_ID, X_HAS_NAME ) ) {
    RwfMsgKeyWriter & k = resp.add_msg_key();
    if ( msg_key.test( X_HAS_SERVICE_ID ) )
      k.service_id( msg_key.service_id );
    if ( msg_key.test( X_HAS_NAME ) )
      k.name( msg_key.name, msg_key.name_len );
    if ( msg_key.test( X_HAS_NAME_TYPE ) )
      k.name_type( msg_key.name_type );
  }
  resp.end_msg();
  this->send_msg( "send_status", resp, temp_buf );
}
