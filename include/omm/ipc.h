#ifndef __rai_omm__ipc_h__
#define __rai_omm__ipc_h__

#include <raikv/array_space.h>
#include <raimd/md_types.h>
#include <raimd/omm_msg.h>

extern "C" const char *omm_get_version( void );

namespace rai {
namespace omm { 

/*
 * protocol
 *
 * client -> init record
 * server -> init record
 * client -> key exchange compute
 * client -> login
 *

client init rec:

|-len-|op|con-version|fl|hd|co|to|fl|pr|ma|mi|host_len
+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
|00 44 00 00 00 00 17 08 1e 00 06 00 00 0e 01 04
+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+

op_code = IPC_INIT_CLIENT(0), client init record
fl = 08, request key exchange when con-version >= 23

server init rec:

|-len-|op|ex|??|??| ripc vers |maxsz|fl|to|ma|micomp|zlib
+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
|00 23 01 01 0a 00 00 00 00 09 18 00 00 3c 0e 01
+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+

op_code  = IPC_EXTENDED(1), ext_code = IPC_CONNACK(1)

client key ack;

|-len-|op||keylen
+--+--+--+--+
|00 04 08 00
+--+--+--+--+

fl = 08, client key exchange ack

client login msg:

|-len-|op|-len-|- message data
+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
|00 74 12 00 6f 00 6d 01 01 00 00 00 01 04 00 80
+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+

op_code = IPC_DATA(2) | IPC_PACKING(16)

|-len-|op|ex| -frag len-|-num-|- message data
+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
|01 31 03 08 00 00 01 27 00 01 01 25 01 01 00 00
+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+

op_code = IPC_DATA(2) | IPC_EXTENDED(1)
extended = EXTENDED_IPC_FRAG(8)
 */

static const uint32_t IPC_CONN_VER_14        = 23,
                      RIPC_VERSION_14        = 9;
static const uint8_t  IPC_SERVER_PING        = 2,
                      IPC_CLIENT_PING        = 1,
                      IPC_EXTENDED_FLAGS     = 1,
                      RSSL_COMP_FORCE        = 0x10, /* extended flag ? */
                      IPC_INIT_CLIENT        = 0,
                      IPC_DATA               = 2,
                      IPC_COMP_DATA          = 4,
                      IPC_COMP_FRAG          = 8,
                      IPC_PACKING            = 0x10, /* pack multiple msgs */
                      EXTENDED_IPC_CONNACK     = 1,
                      EXTENDED_IPC_CONNNAK     = 2,
                      EXTENDED_IPC_FRAG        = 4,
                      EXTENDED_IPC_FRAG_HEADER = 8;
;

struct IpcHdr {
  enum Status {
    BAD_IPC_LEN     = -3,
    NOT_ENOUGH_DATA = -2,
    NOT_ENOUGH_HDR  = -1,
    MSG_OK          = 0,
    MSG_PACKED      = 1,
    MSG_EXTENDED    = 2
  };
  size_t   ipc_len,
           header_len,
           extended_len,
           next_off;
  uint16_t frag_id;
  uint8_t  op_code,
           extended_op;

  Status parse( const uint8_t *buf,  size_t len ) {
    if ( len < 3 )
      return NOT_ENOUGH_HDR;

    this->ipc_len = ( (size_t) buf[ 0 ] << 8 ) | (size_t) buf[ 1 ];
    this->op_code = buf[ 2 ];

    if ( this->ipc_len < 3 )
      return BAD_IPC_LEN;
    if ( this->ipc_len > len )
      return NOT_ENOUGH_DATA;

    this->extended_len = 0;
    this->next_off     = this->ipc_len;
    this->header_len   = 3;
    this->frag_id      = 0;
    this->extended_op  = 0;

    if ( ( this->op_code & IPC_EXTENDED_FLAGS ) != 0 ) {
      size_t i = 3;
      this->extended_op = buf[ i++ ];
      if ( ( this->extended_op & EXTENDED_IPC_FRAG_HEADER ) != 0 ) {
        if ( len < 4 + 4 )
          return NOT_ENOUGH_HDR;
        this->extended_len = ( (size_t) buf[ 4 ] << 24 ) |
                             ( (size_t) buf[ 5 ] << 16 ) |
                             ( (size_t) buf[ 6 ] << 8 ) |
                             ( (size_t) buf[ 7 ] );
        i += 4;
      }
      if ( ( this->extended_op & ( EXTENDED_IPC_FRAG |
                                   EXTENDED_IPC_FRAG_HEADER ) ) != 0 ) {
        if ( i + 2 > len )
          return NOT_ENOUGH_HDR;
        this->frag_id      = ( (uint16_t) buf[ i ] << 8 ) |
                             ( (uint16_t) buf[ i+1 ] );
        i += 2;
      }
      this->header_len = i;
      if ( this->extended_len + this->header_len == this->ipc_len )
        return MSG_OK;
      return MSG_EXTENDED;
    }
    if ( ( this->op_code & IPC_PACKING ) != 0 ) {
      if ( len < 5 )
        return NOT_ENOUGH_HDR;
      size_t pack_len = ( (size_t) buf[ 3 ] << 8 ) | (size_t) buf[ 4 ];
      this->header_len += 2;
      if ( pack_len + this->header_len < this->ipc_len ) {
        this->next_off = this->header_len + pack_len;
        return MSG_PACKED;
      }
    }
    return MSG_OK;
  }

  bool is_conn_init( void ) const {
    return this->op_code == IPC_INIT_CLIENT;
  }
  bool is_conn_ack( void ) const {
    return this->op_code == IPC_EXTENDED_FLAGS &&
           this->extended_op == EXTENDED_IPC_CONNACK;
  }
  bool is_data( void ) const {
    return ( this->op_code & IPC_DATA ) != 0;
  }
  void print( void *buf ) noexcept;
};

struct IpcFrag {
  uint32_t extended_len,
           frag_off;
  uint16_t frag_id;
  char   * msg_buf;

  IpcFrag() : extended_len( 0 ), frag_off( 0 ), frag_id( 0 ),
              msg_buf( 0 ) {}
  bool merge( uint16_t frag_id, uint32_t ext_len,  char *&msg,
              size_t &len ) noexcept;
};

struct ClientInitRec {
  uint8_t  op_code;      /* 0 / 1 = conn_ack 2 = conn_nak 4 = frag 8 = frag_h */
  uint32_t conn_ver;     /* 23 = ver 14 */
  uint8_t  key_ex;       /* 0x08 = key exchange */
  uint8_t  ping_timeout; /* 60 */
  uint8_t  rssl_flags;   /* 0x1 = svr ping, 0x2 clnt ping, 0x80 = comp force */
  uint8_t  protocol;     /* 0 == socket */
  uint8_t  major;        /* 0xe */
  uint8_t  minor;        /* 0x1 */
  uint8_t  host_len;     /* 4 */
  char     host[ 256 ];   /* chex */
  uint8_t  ip_addr_len;  /* 9 */
  char     ip_addr[ 64 ];/* 127.0.0.1 */
  uint8_t  comp_len;     /* 0x23 35 */
  char     comp_ver[ 256 ]; /* */

  ClientInitRec() {
    ::memset( (void *) this, 0, sizeof( *this ) );
    this->op_code      = IPC_INIT_CLIENT;
    this->conn_ver     = IPC_CONN_VER_14;
    this->ping_timeout = 60;
    this->major        = 14;
    this->minor        = 1;
  }

  size_t pack_len( void ) const {
    return 19 + this->host_len + this->ip_addr_len + this->comp_len;
  }
  void pack( char *p ) const {
    kv::CatPtr out( p );

/*0*/out.n16( this->pack_len() )
/*2*/   .n8 ( this->op_code )
/*3*/   .n32( this->conn_ver )
/*7*/   .n8 ( this->key_ex )
/*8*/   .n8 ( 15 + this->host_len + this->ip_addr_len + 2 )
/*9*/   .n8 ( 0 ) /* compresssion bitmap sz */
/*10*/  .n8 ( this->ping_timeout )
/*11*/  .n8 ( this->rssl_flags )
/*12*/  .n8 ( this->protocol )
/*13*/  .n8 ( this->major )
/*14*/  .n8 ( this->minor )
/*15*/  .n8 ( this->host_len )
/*16*/  .b  ( this->host, this->host_len )
/*  */  .n8 ( this->ip_addr_len )
/*  */  .b  ( this->ip_addr, this->ip_addr_len )
/*  */  .n8 ( this->comp_len + 1 )
/*  */  .n8 ( this->comp_len )
/*  */  .b  ( this->comp_ver, this->comp_len );
  }
  bool unpack( char *p,  size_t len ) {
    md::RwfDecoder inp( (uint8_t *) p, (uint8_t *) &p[ len ] );
    uint16_t sz = 0;
    uint8_t  hdr_size,
             compr_bitmap_size,
             comp_len_size;
    inp.u16 ( sz )
       .u8  ( this->op_code )
       .u32 ( this->conn_ver )
       .u8  ( this->key_ex )
       .u8  ( hdr_size ) /* offset to comp_ver */
       .u8  ( compr_bitmap_size )
       .incr( compr_bitmap_size )
       .u8  ( this->ping_timeout )
       .u8  ( this->rssl_flags )
       .u8  ( this->protocol )
       .u8  ( this->major )
       .u8  ( this->minor )
       .u8  ( this->host_len );

    ::memcpy( this->host, inp.buf, this->host_len );
    inp.incr( this->host_len )
       .u8  ( this->ip_addr_len );

    if ( this->ip_addr_len > sizeof( this->ip_addr ) )
      this->ip_addr_len = sizeof( this->ip_addr );
    ::memcpy( this->ip_addr, inp.buf, this->ip_addr_len );

    inp.seek( hdr_size )
       .u8  ( comp_len_size )
       .u8  ( this->comp_len );

    ::memcpy( this->comp_ver, inp.buf, this->comp_len );
    inp.incr( this->comp_len );

    if ( inp.ok ) {
      if ( inp.buf > inp.eob )
        inp.ok = false;
    }
    return inp.ok;
  }
};

struct ServerInitRec {
  uint8_t  op_code,      /* 1 = extended */
           ext_code,     /* 1 = conn ack */
           ipc_10_hdrlen,/* 10 */
           ipc_0;        /* 0 ? */
  uint32_t ripc_ver;     /* 9 = ver 14 */
  uint16_t max_msg_size; /* 6144 = 6 * 1024 */
  uint8_t  rssl_flags,   /* 0, 0x1 = , 0x2 = pings, 0x80 = compression force */
           ping_timeout, /* 60 secs */
           major,        /* 0xe 14 */
           minor;        /* 0x1 1 */
  uint16_t comp;         /* 0 */
  uint8_t  zlib,         /* 0 */
           key_ex,
           enc_type,
           exchg_len,
           comp_len;     /* len of version */
  char     comp_ver[ 256 ];

  ServerInitRec() {
    ::memset( this, 0, sizeof( *this ) );
    this->op_code       = IPC_EXTENDED_FLAGS;
    this->ext_code      = EXTENDED_IPC_CONNACK;
    this->ipc_10_hdrlen = 10;
    this->ripc_ver      = RIPC_VERSION_14;
    this->max_msg_size  = 6144;
    this->ping_timeout  = 60;
    this->major         = 14;
    this->minor         = 1;
  }

  size_t pack_len( void ) const {
    return 24 + this->comp_len;
  }
  void pack( char *p ) const {
    kv::CatPtr out( p );

    out.n16( this->pack_len() )
       .n8 ( this->op_code )
       .n8 ( this->ext_code )
       .n8 ( this->ipc_10_hdrlen )
       .n8 ( this->ipc_0 )  /* always 0 */
       .n32( this->ripc_ver )
       .n16( this->max_msg_size )
       .n8 ( this->rssl_flags )
       .n8 ( this->ping_timeout )
       .n8 ( this->major )
       .n8 ( this->minor )
       .n16( this->comp )
       .n8 ( this->zlib )
       .n8 ( this->key_ex )    /* KEY EX */
       .n8 ( this->enc_type )  /* encrypt type */
       .n8 ( this->exchg_len ) /* exchange len */
       .n8 ( this->comp_len + 2 )
       .n8 ( this->comp_len )
       .b  ( this->comp_ver, this->comp_len );
  }
  bool unpack( char *p,  size_t len ) {
    md::RwfDecoder inp( (uint8_t *) p, (uint8_t *) &p[ len ] );
    uint16_t sz = 0;
    inp.u16( sz );
    inp.u8 ( this->op_code )
       .u8 ( this->ext_code )
       .u8 ( this->ipc_10_hdrlen ) /* IPC_100_CONN_ACK hdr length */
       .u8 ( this->ipc_0 )
       .u32( this->ripc_ver )
       .u16( this->max_msg_size )
       .u8 ( this->rssl_flags )
       .u8 ( this->ping_timeout )
       .u8 ( this->major )
       .u8 ( this->minor )
       .u16( this->comp )
       .u8 ( this->zlib );
    if ( this->key_ex != 0 ) {
      inp.u8 ( this->key_ex ) /* KEY EX */
         .u8 ( this->enc_type ) /* encrypt type */
         .u8 ( this->exchg_len );/* exchange len, 24 */
      /*
      if ( this->exchg_len == 24 ) {
        inp.u64( this->P )
           .u64( this->G )
           .u64( this->key );
      }
      * needs key exchange reply */
      inp.ok = false;
    }
    inp.u8 ( this->comp_len ) /* out */
       .u8 ( this->comp_len );
    if ( inp.ok ) {
      if ( &inp.buf[ this->comp_len ] > inp.eob )
        inp.ok = false;
      else {
        ::memcpy( this->comp_ver, inp.buf, this->comp_len );
        this->comp_ver[ this->comp_len ] = '\0'; /* always < 256 */
      }
    }
    return inp.ok;
  }
};

template <class T>
static void
init_component_string( T &rec ) {
  ::strcpy( rec.comp_ver, "omm-" );
  ::strcpy( &rec.comp_ver[ 4 ], omm_get_version() );
  rec.comp_len = ::strlen( rec.comp_ver );
}
#if 0
struct client_keyexch {
  uint16_t len;         /* 12 */
  uint8_t  op_code;     /* RIPC_KEY_EXCHANGE */
  uint8_t  key_len;     /* 8 */
  uint64_t send_key;    /* modPowFast( g, rand, p ) */
};
#endif

/*maxMsgSize = 6161 = 6144 + 3 + 14 (RWS_MAX_HEADER)*/
}
}

#endif
