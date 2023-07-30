#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <raimd/rwf_msg.h>
#include <raimd/md_dict.h>
#include <raimd/app_a.h>
#include <raimd/enum_def.h>
#include <raimd/cfile.h>
#include <omm/ev_omm.h>
#include <omm/ev_omm_client.h>
#include <omm/src_dir.h>

using namespace rai;
using namespace kv;
using namespace md;
using namespace omm;

void
EvOmmClient::send_dictionary_request( void ) noexcept
{
  MDMsgMem mem;
  const char * fld_dict_name  = NULL,
             * enum_dict_name = NULL;
  uint32_t     fld_dict_svc   = 0,
               enum_dict_svc  = 0;
  TempBuf      temp_buf;

  if ( this->dict_in_progress != NULL ) {
    fprintf( stderr, "dictionary already in progress\n" );
    return;
  }
  OmmSourceDB &db = this->source_db;
  for ( size_t i = 0; i < db.source_list.count; i++ ) {
    OmmSource * src;
    for ( src = db.source_list.ptr[ i ].hd; src != NULL; src = src->next ) {
      if ( fld_dict_name == NULL && src->info.num_dict > 0 ) {
        fld_dict_name = src->info.dictionaries_provided[ 0 ]; /* 0 = fld */
        fld_dict_svc  = src->service_id;
      }
      if ( enum_dict_name == NULL && src->info.num_dict > 1 ) {
        enum_dict_name = src->info.dictionaries_provided[ 1 ];/* 1 = enum */
        enum_dict_svc  = src->service_id;
      }
      if ( fld_dict_name != NULL && enum_dict_name != NULL )
        goto break_loop;
    }
  }
break_loop:;
  if ( fld_dict_name != NULL ) {
    temp_buf = this->mktemp( 256 );
    RwfMsgWriter drq( mem, NULL, temp_buf.msg, temp_buf.len, REQUEST_MSG_CLASS,
                      DICTIONARY_DOMAIN, dictionary_stream_id );
    drq.add_msg_key()
       .service_id( fld_dict_svc )
       .name( fld_dict_name )
       .name_type( NAME_TYPE_RIC )
       .filter( DICT_VERB_NORMAL )
    .end_msg();

    this->send_msg( "fld_reqeust", drq, temp_buf );
  }
  if ( enum_dict_name != NULL ) {
    temp_buf = this->mktemp( 256 );
    RwfMsgWriter erq( mem, NULL, temp_buf.msg, temp_buf.len, REQUEST_MSG_CLASS,
                      DICTIONARY_DOMAIN, enumdefs_stream_id );
    erq.add_msg_key()
       .service_id( enum_dict_svc )
       .name( enum_dict_name )
       .name_type( NAME_TYPE_RIC )
       .filter( DICT_VERB_NORMAL )
    .end_msg();

    this->send_msg( "enum_request", erq, temp_buf );
  }
  if ( fld_dict_name != NULL || enum_dict_name != NULL ) {
    DictInProg * p = new ( ::malloc( sizeof( DictInProg ) ) ) DictInProg();
    p->dict_in_progress += ( fld_dict_name != NULL );
    p->dict_in_progress += ( enum_dict_name != NULL );
    p->fld_dict_name     = fld_dict_name;
    p->enum_dict_name    = enum_dict_name;
    p->field_stream_id   = dictionary_stream_id;
    p->enum_stream_id    = enumdefs_stream_id;
    this->dict_in_progress = p;
  }
}

void
EvOmmClient::recv_dictionary_response( RwfMsg &msg ) noexcept
{
  RwfMsgHdr  & hdr = msg.msg;
  DictInProg * p = this->dict_in_progress;
  if ( p == NULL ) {
    fprintf( stderr, "no dictionary in progress\n" );
    return;
  }
  RwfMsg * series = msg.get_container_msg(),
         * summary;
  if ( series == NULL || series->base.type_id != RWF_SERIES ) {
    fprintf( stderr, "dictionary is not a series\n" );
    return;
  }
  bool is_field_dict = ( hdr.stream_id == p->field_stream_id );
  if ( (summary = series->get_summary_msg()) != NULL ) {
    MDFieldReader rd( *summary );
    uint8_t type;
    if ( rd.find( "Type" ) && rd.get_uint( type ) ) {
      bool is_flipped = false;
      switch ( type ) {
        case 1: is_flipped = ! is_field_dict; break;
        case 2: is_flipped = is_field_dict;   break;
        default: break;
      }
      if ( is_flipped ) {
        is_field_dict = ! is_field_dict;
        uint32_t tmp = p->enum_stream_id;
        p->enum_stream_id  = p->field_stream_id;
        p->field_stream_id = tmp;
      }
    }
  }
  if ( is_field_dict ) {
    int status = RwfMsg::decode_field_dictionary( p->dict_build, *series );
    if ( status != 0 )
      fprintf( stderr, "failed to get field dictionary, status %d\n", status );
  }
  else {
    int status = RwfMsg::decode_enum_dictionary( p->dict_build, *series );
    if ( status != 0 )
      fprintf( stderr, "failed to get enum dictionary, status %d\n", status );
  }
  if ( hdr.test( X_REFRESH_COMPLETE ) ) {
    if ( --p->dict_in_progress == 0 ) {
      p->dict_build.index_dict( "app_a", this->dict.rdm_dict );
      print_dict_info( this->dict.rdm_dict, p->fld_dict_name,
                       p->enum_dict_name );
      p->dict_build.clear_build();
      this->dict_in_progress = NULL;
      delete p;
    }
  }
}

void
EvOmmService::recv_dictionary_request( RwfMsg &msg ) noexcept
{
  RwfMsgHdr & hdr = msg.msg;
  if ( is_omm_debug )
    debug_print( "dictionary_request", msg );

  MDDict * dict = this->dict.rdm_dict;
  if ( dict == NULL ) {
    this->send_status( msg, STATUS_CODE_NOT_FOUND );
    return;
  }
  RwfMsgKey & msg_key       = hdr.msg_key;
  OmmSource * source        = NULL;
  MDFid       fid_start     = dict->min_fid,
              fid_end       = 0;
  uint32_t    map_start     = 1,
              map_end       = 0;
  uint8_t     verb          = DICT_VERB_NORMAL;
  bool        is_done       = false,
              is_first      = true,
              is_field_dict = ( msg_key.name_len == 6 &&
                                ::memcmp( msg_key.name, "RWFFld", 6 ) == 0 ),
              is_enum_dict  = ( msg_key.name_len == 7 &&
                                ::memcmp( msg_key.name, "RWFEnum", 7 ) == 0 );

  if ( msg_key.test( X_HAS_FILTER ) )
    verb = msg_key.filter;
  source = this->source_db.find_source( msg_key.service_id, 0 );

  for ( ; source != NULL; source = source->next ) {
    const char * dict1 = source->info.dictionaries_provided[ 0 ],
               * dict2 = source->info.dictionaries_provided[ 1 ];
    if ( source->info.num_dict > 0 &&
         ::memcmp( dict1, msg_key.name, msg_key.name_len ) == 0 &&
         msg_key.name_len == ::strlen( dict1 ) ) {
      is_field_dict = true;
      is_enum_dict  = false;
      break;
    }
    else if ( source->info.num_dict > 1 &&
         ::memcmp( dict2, msg_key.name, msg_key.name_len ) == 0 &&
         msg_key.name_len == ::strlen( dict2 ) ) {
      is_field_dict = false;
      is_enum_dict  = true;
      break;
    }
  }
  if ( ! is_field_dict && ! is_enum_dict ) {
    this->send_status( msg, STATUS_CODE_NOT_FOUND );
    return;
  }
  do {
    MDMsgMem     mem;
    TempBuf      temp_buf = this->mktemp( 8 * 1024 );
    RwfMsgWriter resp( mem, dict, temp_buf.msg, temp_buf.len,
                      REFRESH_MSG_CLASS, DICTIONARY_DOMAIN, hdr.stream_id );
    char         descr[ 64 ];
    CatPtr       pd( descr );

    if ( is_field_dict )
      pd.s( "Dict Refresh, fid " ).i( fid_start ).end();
    else
      pd.s( "Enum Refresh, map " ).u( map_start ).end();

    if ( is_first )
      resp.set( X_CLEAR_CACHE );

    resp.set( X_SOLICITED )
       .add_state( DATA_STATE_OK, STREAM_STATE_OPEN, descr )
       .add_msg_key()
         .service_id( msg_key.service_id )
         .name( msg_key.name, msg_key.name_len )
         .name_type( msg_key.name_type )
         .filter( msg_key.filter )
       .end_msg_key();
    RwfSeriesWriter &ser = resp.add_series();

    if ( is_field_dict ) {
      ser.encode_field_dictionary( fid_start, fid_end, verb, is_first, 4096 );
      fid_start = fid_end;
      if ( fid_start >= dict->max_fid ) {
        resp.set( X_REFRESH_COMPLETE );
        is_done = true;
      }
    }
    else {
      ser.encode_enum_dictionary( map_start, map_end, verb, is_first, 4096 );
      map_start = map_end;
      if ( map_start >= dict->map_count ) {
        resp.set( X_REFRESH_COMPLETE );
        is_done = true;
      }
    }
    resp.end_msg();
    this->send_msg( "dictionary_response", resp, temp_buf );
    is_first = false;
  } while ( ! is_done );
}

void
rai::omm::print_dict_info( MDDict *d,  const char *fld_dict_name,
                           const char *enum_dict_name ) noexcept
{
  /*DictInProg * p = this->dict_in_progress;*/
  char buf[ 120 ];
  CatPtr x( buf );

  x.w( 15, "Field Dict" )
   .w( 15, "Enum Dict" )
   .w( 12, "Fid Count" )
   .w( 10, "Fid Min" )
   .w( 10, "Fid Max" )
   .w( 12, "Dict Size" )
   .end();
  printf( "%s\n", buf );

  int32_t f = uint32_digits( d->entry_count ),
          a = int32_digits( d->min_fid ),
          b = int32_digits( d->max_fid ),
          z = uint32_digits( d->dict_size );
  const char * fld_name  = fld_dict_name,
             * enum_name = enum_dict_name;
  if ( fld_name == NULL )
    fld_name = "";
  if ( enum_name == NULL )
    enum_name = "";

  x.begin();
  x.w( 15, fld_name )
   .w( 15, enum_name )
   .u( d->entry_count, f ).w( 12 - f, "" )
   .i( d->min_fid, a ).w( 10 - a, "" )
   .i( d->max_fid, b ).w( 10 - b, "" )
   .u( d->dict_size, z ).w( 12 - z, "" )
   .end();
  printf( "%s\n\n", buf );
  fflush( stdout );
}

bool
OmmDict::load_cfiles( const char *path ) noexcept
{
  bool have_dictionary = false;
  if ( path != NULL || (path = ::getenv( "cfile_path" )) != NULL ) {
    MDDictBuild dict_build;
    /*dict_build.debug_flags = MD_DICT_PRINT_FILES;*/
    if ( AppA::parse_path( dict_build, path, "RDMFieldDictionary" ) == 0 ) {
      EnumDef::parse_path( dict_build, path, "enumtype.def" );
      dict_build.index_dict( "app_a", this->rdm_dict );
    }
    if ( this->rdm_dict != NULL && this->rdm_dict->dict_type[ 0 ] == 'a' )
      have_dictionary = true;
    dict_build.clear_build();
    if ( CFile::parse_path( dict_build, path, "tss_fields.cf" ) == 0 ) {
      CFile::parse_path( dict_build, path, "tss_records.cf" );
      dict_build.index_dict( "cfile", this->cfile_dict );
      this->cfile_dict->next = this->rdm_dict;
    }
  }
  return have_dictionary;
}
