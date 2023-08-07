#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <raimd/rwf_msg.h>
#include <raimd/md_dict.h>
#include <raimd/app_a.h>
#include <omm/ev_omm.h>
#include <omm/ev_omm_client.h>
#include <omm/src_dir.h>

using namespace rai;
using namespace kv;
using namespace md;
using namespace omm;

void
EvOmmService::recv_login_request( RwfMsg &msg ) noexcept
{
  RwfMsgHdr & hdr = msg.msg;
  LoginInfo * info = this->login;
  uint32_t    stream_id = hdr.stream_id;

  if ( is_omm_debug )
    debug_print( "login_request", msg );

  if ( hdr.msg_class == REQUEST_MSG_CLASS ) {
    if ( info == NULL ) {
      info = new ( ::malloc( sizeof( LoginInfo ) ) ) LoginInfo( stream_id );
      this->login = info;
    }
    TempBuf      temp_buf = this->mktemp( 1024 );
    MDMsgMem     mem;
    RwfMsg     * atts;
    RwfMsgWriter resp( mem, NULL, temp_buf.msg, temp_buf.len, /* spc for ipc hdr */
                       REFRESH_MSG_CLASS, LOGIN_DOMAIN, stream_id );
    RwfMsgKey  & msg_key = hdr.msg_key;
    size_t       len;

    info->stream_id = stream_id;
    if ( (len = msg_key.name_len) > 0 ) {
      if ( len > sizeof( info->user ) - 1 )
        len = sizeof( info->user ) - 1;
      ::memcpy( info->user, msg_key.name, len );
    }
    if ( (atts = msg.get_msg_key_attributes()) != NULL ) {
      MDIterMap map[ MAX_OMM_ITER_FIELDS ];
      MDIterMap::get_map( *atts, map, info->iter_map( map ) );
    }
    resp.set( X_STREAMING, X_CLEAR_CACHE, X_REFRESH_COMPLETE, X_SOLICITED )
       .add_state( DATA_STATE_OK, STREAM_STATE_OPEN, "Login accepted" )
       .add_msg_key()
         .name( info->user )
         .name_type( NAME_TYPE_USER_NAME )
         .attrib()
           .apply( *this, &EvOmmService::add_login_response_attrs, info )
    .end_msg();

    this->send_msg( "login_response", resp, temp_buf );
  }
  else if ( hdr.msg_class == CLOSE_MSG_CLASS ) {
    if ( info != NULL && info->stream_id == stream_id )
      info->stream_id = 0;
  }
  else {
    this->send_status( msg, STATUS_CODE_USAGE_ERROR, "Not supported" );
  }
}

RwfElementListWriter &
EvOmmService::add_login_response_attrs( RwfElementListWriter &elist,
                                        LoginInfo *info ) noexcept
{
  if ( info->application_id[ 0 ] != '\0' )
    elist.append_string( APP_ID, info->application_id );
  if ( info->application_name[ 0 ] != '\0' )
    elist.append_string( APP_NAME, info->application_name );
  if ( info->position[ 0 ] != '\0' )
    elist.append_string( POSITION, info->position );

  elist.append_uint( PROV_PERM_PROF , 0 ) /* no dacs */
       .append_uint( PROV_PERM_EXPR , 0 )
       .append_uint( SINGLE_OPEN    , 1 ) /* consumer drives */
       .append_uint( ALLOW_SUSPECT  , 1 );/* stream state */
  return elist;
}

void
EvOmmClient::send_login_request( void ) noexcept
{
  TempBuf temp_buf = this->mktemp( 1024 );
  MDMsgMem mem;
  RwfMsgWriter msg( mem, NULL, temp_buf.msg, temp_buf.len,
                    REQUEST_MSG_CLASS, LOGIN_DOMAIN, login_stream_id );

  msg.set( X_STREAMING )
     .add_msg_key()
       .name( this->user )
       .name_type( NAME_TYPE_USER_NAME )
       .attrib()
         .apply( *this, &EvOmmClient::add_login_request_attrs )
  .end_msg();

  this->send_msg( "login_request", msg, temp_buf );
}

RwfElementListWriter &
EvOmmClient::add_login_request_attrs( RwfElementListWriter &elist ) noexcept
{
  if ( this->app_id != NULL )
    elist.append_string( APP_ID, this->app_id );
  if ( this->app_name != NULL )
    elist.append_string( APP_NAME, this->app_name );
  if ( this->pass != NULL )
    elist.append_string( PASSWD, this->pass );
  if ( this->token != NULL )
    elist.append_string( AUTH_TOKEN, this->token );

  elist.append_uint( PROV_PERM_PROF, 1 ) /* dacs */
       .append_uint( PROV_PERM_EXPR, 1 )
       .append_uint( SUP_PROV_DICT , 1 ) /* support provider dictionary */
       .append_uint( SINGLE_OPEN   , 1 ) /* consumer drives */
       .append_uint( ALLOW_SUSPECT , 1 );/* has stream state */
  /* other options default to zero */
  if ( this->instance_id != NULL )
    elist.append_string( INSTANCE_ID, this->instance_id );
  elist.append_uint( ROLE, 0 ); /* 0: consumer, 1: provider */
  return elist;
}

bool
EvOmmClient::recv_login_response( RwfMsg &msg ) noexcept
{
  RwfMsgHdr & hdr = msg.msg;
  if ( is_omm_debug )
    debug_print( "login_response", msg );

  if ( hdr.msg_class != REFRESH_MSG_CLASS )
    return rejected( "login_response", msg );

  this->login = new ( ::malloc( sizeof( LoginInfo ) ) )
    LoginInfo( login_stream_id );
  RwfMsgKey & msg_key = hdr.msg_key;
  size_t len;
  if ( (len = msg_key.name_len) > 0 ) {
    if ( len > sizeof( this->login->user ) - 1 )
      len = sizeof( this->login->user ) - 1;
    ::memcpy( this->login->user, msg_key.name, len );
  }
  RwfMsg *elem = msg.get_msg_key_attributes();
  if ( elem != NULL && elem->base.type_id == RWF_ELEMENT_LIST ) {
    MDIterMap map[ MAX_OMM_ITER_FIELDS ];
    MDIterMap::get_map( *elem, map, this->login->iter_map( map ) );
  }
  return true;
}

