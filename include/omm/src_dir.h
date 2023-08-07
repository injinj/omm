#ifndef __rai_omm__src_dir_h__
#define __rai_omm__src_dir_h__

namespace rai {
using namespace md;
namespace omm {

static const size_t MAX_CAPABILITIES   = 32,
                    MAX_QOS            = 5,
                    MAX_DICTIONARIES   = 5,
                    MAX_GROUP_INFO_LEN = 64,
                    MAX_DATA_INFO_LEN  = 64,
                    MAX_LINKS          = 5,
                    MAX_OMM_STRLEN     = 128, /* user, name, pass, svc_name, vendor, ... */
                    MAX_DICT_STRLEN    = 32,
                    MAX_OMM_ITER_FIELDS= 32; /* map[] size */

static const char APP_ID[]         = "ApplicationId",
                  APP_NAME[]       = "ApplicationName",
                  PASSWD[]         = "Password",
                  INSTANCE_ID[]    = "InstanceId",
                  POSITION[]       = "Position",
                  PROV_PERM_PROF[] = "ProvidePermissionProfile",
                  PROV_PERM_EXPR[] = "ProvidePermissionExpressions",
                  SINGLE_OPEN[]    = "SingleOpen",
                  SUP_PROV_DICT[]  = "SupportProviderDictionaryDownload",
                  ALLOW_SUSPECT[]  = "AllowSuspectData",
                  SUP_OPT_PAUSE[]  = "SupportOptimizedPauseResume",
                  SUP_OMM_POST[]   = "SupportOMMPost",
                  SUP_VIEW_REQ[]   = "SupportViewRequests",
                  SUP_BATCH_REQ[]  = "SupportBatchRequests",              
                  SUP_STANDBY[]    = "SupportStandby",
                  ROLE[]           = "Role",
                  AUTH_TOKEN[]     = "AuthenticationToken";
struct LoginInfo {
  char     user[ MAX_OMM_STRLEN ],             /*  req  | refr | def | mean */
           application_id[ 16 ],               /*   x   |  x   |     | dacs */
           application_name[ MAX_OMM_STRLEN ], /*   x   |  x   |     | identity */
           position[ 32 ],                     /*   x   |  x   |     | dacs */
           password[ MAX_OMM_STRLEN ],         /*   x   |      |     | identity */
           instance_id[ 32 ];                  /*   x   |      |     | identity */
  uint8_t  provide_permission_profile,         /*   x   |  x   |  1  | perm profile */
           provide_permission_expressions,     /*   x   |  x   |  1  | perm expr in resp */
           single_open,                        /*   x   |  x   |  1  | consumer drive stream recovery */
           support_prov_dict_download,         /*   x   |  x   |  0  | adh download dict, no src */
           allow_suspect_data,                 /*   x   |  x   |  1  | allow stream state / stream close */
           support_optimized_pause_resume,     /*   x   |  x   |  0  | pause via login stream */
           support_omm_post,                   /*       |  x   |  0  | prov supports posting */
           support_view_requests,              /*       |  x   |  0  | supports views */
           support_standby,                    /*       |  x   |  0  | prov runs in FT standby mode */
           role;                               /*   x   |      |  0  | consumer:0 /provider:1 */
  uint32_t support_batch_requests,             /*       |  x   |  0  | prov supports batches */
           stream_id;
  /* RTT */
  /* seq_num, seq_num_recovery, seq_num_retry_interval */
  /* download_connection_config */
  /* support_enhanced_symbol_list, watch individual symbols */
  /* other authn related fields */
  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }
  LoginInfo( uint32_t id ) {
    ::memset( (void *) this, 0, sizeof( *this ) );
    this->stream_id = id;
  }
  size_t iter_map( MDIterMap *mp ) {
    size_t n = 0;
    mp[ n++ ].string( APP_ID        ,  application_id                , sizeof( application_id ) );
    mp[ n++ ].string( APP_NAME      ,  application_name              , sizeof( application_name ) );
    mp[ n++ ].string( PASSWD        ,  password                      , sizeof( password ) );
    mp[ n++ ].string( INSTANCE_ID   ,  instance_id                   , sizeof( instance_id ) );
    mp[ n++ ].string( POSITION      ,  position                      , sizeof( position ) );
    mp[ n++ ].uint  ( PROV_PERM_PROF, &provide_permission_profile    , sizeof( provide_permission_profile ) );
    mp[ n++ ].uint  ( PROV_PERM_EXPR, &provide_permission_expressions, sizeof( provide_permission_expressions ) );
    mp[ n++ ].uint  ( SINGLE_OPEN   , &single_open                   , sizeof( single_open ) );
    mp[ n++ ].uint  ( SUP_PROV_DICT , &support_prov_dict_download    , sizeof( support_prov_dict_download ) );
    mp[ n++ ].uint  ( ALLOW_SUSPECT , &allow_suspect_data            , sizeof( allow_suspect_data ) );
    mp[ n++ ].uint  ( SUP_OPT_PAUSE , &support_optimized_pause_resume, sizeof( support_optimized_pause_resume ) );
    mp[ n++ ].uint  ( SUP_OMM_POST  , &support_omm_post              , sizeof( support_omm_post ) );
    mp[ n++ ].uint  ( SUP_VIEW_REQ  , &support_view_requests         , sizeof( support_view_requests ) );
    mp[ n++ ].uint  ( SUP_BATCH_REQ , &support_batch_requests        , sizeof( support_batch_requests ) );
    mp[ n++ ].uint  ( SUP_STANDBY   , &support_standby               , sizeof( support_standby ) );
    mp[ n++ ].uint  ( ROLE          , &role                          , sizeof( role ) );
    return n;
  }
};

typedef char QosBuf[ 8 ];
typedef char StateBuf[ 64 ];

static const char NAME[]         = "Name",
                  VEND[]         = "Vendor",
                  IS_SRC[]       = "IsSource",
                  CAPAB[]        = "Capabilities",
                  DICT_PROV[]    = "DictionariesProvided",
                  DICT_USED[]    = "DictionariesUsed",
                  QOS[]          = "QoS",
                  SUP_QOS_RNG[]  = "SupportsQoSRange",
                  ITEM_LST   []  = "ItemList",
                  SUP_OOB_SNAP[] = "SupportsOutOfBandSnapshots",
                  ACC_CONS_STA[] = "AcceptingConsumerStatus";
/* source directory response information */
/* service general information */
struct ServiceInfo {
  char     service_name[ MAX_OMM_STRLEN ],
           vendor[ MAX_OMM_STRLEN ];
  uint8_t  service_name_len,
           is_source,
           capabilities[ MAX_CAPABILITIES ];
  char     dictionaries_provided[ MAX_DICTIONARIES ][ MAX_DICT_STRLEN ],
           dictionaries_used[ MAX_DICTIONARIES ][ MAX_DICT_STRLEN ];
  RwfQos   qos[ MAX_QOS ];
  uint8_t  supports_qos_range;
  char     item_list[ MAX_OMM_STRLEN ];
  uint8_t  supports_out_of_band_snapshots,
           accepting_consumer_status;
  uint32_t num_capabilities,
           num_dict,
           num_dict_used,
           num_qos;
  uint64_t capabilities_mask;

  bool capability_exists( uint8_t domain_type ) const {
    return ( ( (uint64_t) 1 << domain_type ) & this->capabilities_mask ) != 0;
  }

  ServiceInfo() { this->zero(); }
  size_t iter_map( MDIterMap *mp,  QosBuf *qos_buf ) {
    size_t n = 0;
    mp[ n++ ].string      ( NAME        , service_name          , sizeof( service_name ) );
    mp[ n++ ].string      ( VEND        , vendor                , sizeof( vendor ) );
    mp[ n++ ].uint        ( IS_SRC      , &is_source            , sizeof( is_source ) );
    mp[ n++ ].uint_array  ( CAPAB       , capabilities          , &num_capabilities, sizeof( capabilities )         , sizeof( capabilities[ 0 ] ) );
    mp[ n++ ].string_array( DICT_PROV   , dictionaries_provided , &num_dict        , sizeof( dictionaries_provided ), sizeof( dictionaries_provided[ 0 ] ) );
    mp[ n++ ].string_array( DICT_USED   , dictionaries_used     , &num_dict_used   , sizeof( dictionaries_used )    , sizeof( dictionaries_used[ 0 ] ) );
    mp[ n++ ].opaque_array( QOS         , qos_buf               , &num_qos         , sizeof( QosBuf ) * MAX_QOS     , sizeof( qos[ 0 ] ) );
    mp[ n++ ].uint        ( SUP_QOS_RNG , &supports_qos_range   , sizeof( supports_qos_range ) );
    mp[ n++ ].string      ( ITEM_LST    , item_list             , sizeof( item_list ) );
    mp[ n++ ].uint        ( SUP_OOB_SNAP, &supports_out_of_band_snapshots, sizeof( supports_out_of_band_snapshots ) );
    mp[ n++ ].uint        ( ACC_CONS_STA, &accepting_consumer_status, sizeof( accepting_consumer_status ) );
    return n;
  }
  void zero( void ) { ::memset( (void *) this, 0, sizeof( *this ) ); }
};
static const char SVC_STATE[] = "ServiceState",
                  ACC_REQ[]   = "AcceptingRequests",
                  STAT[]      = "Status";
/* service state information */
struct ServiceStateInfo {
  uint8_t  service_state,
           accepting_requests;
  RwfState status;
  StateBuf buf;

  ServiceStateInfo() { this->zero(); }
  size_t iter_map( MDIterMap *mp ) {
    size_t n = 0;
    mp[ n++ ].uint  ( SVC_STATE, &service_state     , sizeof( service_state ) );
    mp[ n++ ].uint  ( ACC_REQ  , &accepting_requests, sizeof( accepting_requests ) );
    mp[ n++ ].opaque( STAT     , buf                , sizeof( StateBuf ) );
    return n;
  }
  void zero( void ) { ::memset( (void *) this, 0, sizeof( *this ) ); }
};
static const char GROUP[]   = "Group",
                  MRG_GRP[] = "MergedToGroup";
/* service group information */
struct ServiceGroupInfo {
  uint8_t  group[ MAX_GROUP_INFO_LEN ];
  uint8_t  merged_to_group[ MAX_GROUP_INFO_LEN ];
  RwfState status;
  StateBuf buf;

  ServiceGroupInfo() { this->zero(); }
  size_t iter_map( MDIterMap *mp ) {
    size_t n = 0;
    mp[ n++ ].opaque( GROUP  , group          , sizeof( group ) );
    mp[ n++ ].opaque( MRG_GRP, merged_to_group, sizeof( merged_to_group ) );
    mp[ n++ ].opaque( STAT   , buf            , sizeof( StateBuf ) );
    return n;
  }
  void zero( void ) { ::memset( (void *) this, 0, sizeof( *this ) ); }
};
static const char OPEN_LIM[]  = "OpenLimit",
                  OPEN_WIN[]  = "OpenWindow",
                  LOAD_FACT[] = "LoadFactor";
/* service load information */
struct ServiceLoadInfo {
  uint32_t open_limit,
           open_window,
           load_factor;

  ServiceLoadInfo() { this->zero(); }
  size_t iter_map( MDIterMap *mp ) {
    size_t n = 0;
    mp[ n++ ].uint( OPEN_LIM , &open_limit , sizeof( open_limit ) );
    mp[ n++ ].uint( OPEN_WIN , &open_window, sizeof( open_window ) );
    mp[ n++ ].uint( LOAD_FACT, &load_factor, sizeof( load_factor ) );
    return n;
  }
  void zero( void ) { ::memset( (void *) this, 0, sizeof( *this ) ); }
};
static const char TYPE[] = "Type",
                  DATA[] = "Data";
/* service data information */
struct ServiceDataInfo {
  uint8_t type,
          data[ MAX_DATA_INFO_LEN ];

  ServiceDataInfo() { this->zero(); }
  size_t iter_map( MDIterMap *mp ) {
    size_t n = 0;
    mp[ n++ ].uint  ( TYPE, &type, sizeof( type ) );
    mp[ n++ ].opaque( DATA,  data, sizeof( data ) ); /* may have different type */
    return n;
  }
  void zero( void ) { ::memset( (void *) this, 0, sizeof( *this ) ); }
};
static const char LINK_STATE[] = "LinkState",
                  LINK_CODE[]  = "LinkCode",
                  TEXT[]       = "Text";
/* service link information */
struct ServiceLinkInfo {
  char    link_name[ MAX_OMM_STRLEN ];
  uint8_t type,      /* LINK_INTERACTIVE, LINK_BROADCAST */
          link_state, /* LINK_UP, LINK_DOWN */
          link_code;  /* LINK_OK, LINK_RECOVER_... */
  char    text[ MAX_OMM_STRLEN ];

  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }
  ServiceLinkInfo() { this->zero(); }
  size_t iter_map( MDIterMap *mp ) {
    size_t n = 0;
    mp[ n++ ].uint(   TYPE      , &type      , sizeof( type ) );
    mp[ n++ ].uint(   LINK_STATE, &link_state, sizeof( link_state ) );
    mp[ n++ ].uint(   LINK_CODE , &link_code , sizeof( link_code ) );
    mp[ n++ ].string( TEXT      , text       , sizeof( text ) );
    return n;
  }
  void zero( void ) { ::memset( (void *) this, 0, sizeof( *this ) ); }
};

struct OmmSource {
  OmmSource       * next,
                  * back;
  uint64_t          origin;
  uint32_t          service_id,
                    filter;
  ServiceInfo       info;
  ServiceStateInfo  state;
  ServiceGroupInfo  group;
  ServiceLoadInfo   load;
  ServiceDataInfo   data;
  ServiceLinkInfo * link[ MAX_LINKS ];
  uint32_t          link_cnt;

  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }
  OmmSource( uint64_t orig,  uint32_t id )
    : next( 0 ), back( 0 ), origin( orig ), service_id( id ),
      filter( 0 ), link_cnt( 0 ) {
    for ( size_t i = 0; i < MAX_LINKS; i++ )
      this->link[ i ] = NULL;
  }
  void clear_info( uint32_t info_id ) noexcept;
  bool find_link( const char *key,  size_t keylen,
                  ServiceLinkInfo *&linkp ) noexcept;
  bool make_link( const char *key,  size_t keylen,
                  ServiceLinkInfo *&linkp ) noexcept;
  void pop_link( ServiceLinkInfo *linkp ) noexcept;
  void print_info( bool hdr ) noexcept;
};

struct SourceDictionary {
  char     dictionaries[ MAX_DICTIONARIES ][ MAX_DICT_STRLEN ];
  uint32_t num_dict;

  SourceDictionary() : num_dict( 0 ) {}
};

struct SourceRoute {
  uint32_t   service_id,
           * next_service,
             hash;
  uint16_t   service_cnt;
  uint8_t    domain;
  uint16_t   len;
  char       value[ 2 ]; /* FEED.REC, FEED.MBO, ... */
};

typedef struct kv::DLinkList<OmmSource> SourceList;

struct OmmSourceDB {
  kv::RouteVec<SourceRoute>     source_sector_tab;
  kv::ArrayCount<SourceList, 4> source_list;
  kv::UIntHashTab             * service_ht;

  OmmSourceDB() : service_ht( 0 ) {
    this->service_ht = kv::UIntHashTab::resize( NULL );
  }

  void add_source( OmmSource *src ) {
    size_t   pos;
    uint32_t i;
    if ( this->service_ht->find( src->service_id, pos, i ) ) {
      SourceList & list = this->source_list.ptr[ i ];
      list.push_tl( src );
      return;
    }
    i = this->source_list.count;
    SourceList & list = this->source_list.push();
    list.push_tl( src );
    this->service_ht->set_rsz( this->service_ht, src->service_id, pos, i );
  }
  OmmSource * find_source( uint32_t service_id,  uint64_t origin ) {
    size_t   pos;
    uint32_t i;
    if ( this->service_ht->find( service_id, pos, i ) ) {
      OmmSource * src = this->source_list.ptr[ i ].hd;
      while ( src != NULL ) {
        if ( origin == 0 || src->origin == origin )
          return src;
        src = src->next;
      }
    }
    return NULL;
  }
  void drop_sources( uint64_t origin ) noexcept;
  uint32_t update_source_map( uint64_t origin,  RwfMsg &map ) noexcept;
  bool update_source_entry( uint64_t origin,  uint32_t service_id,
                            RwfMsg &entry ) noexcept;
  bool update_service_info( uint64_t origin,  uint32_t service_id,
                            uint32_t info_id,  bool is_filter_update,
                            RwfMsg &info ) noexcept;
  void clear_service_info( uint64_t origin,  uint32_t service_id,
                           uint32_t info_id ) noexcept;
  void index_domains( void ) noexcept;
  OmmSource * match_sub( const char *&sub,  size_t &len,
                         uint8_t &domain,  uint64_t origin ) noexcept;
  void print_sources( void ) noexcept;
};

static inline size_t rdm_sector_strlen( const char *sector ) {
  size_t len = 0;
  if ( sector != NULL ) {
    while ( sector[ len ] != '\0' )
      len++;
  }
  return len;
}
#ifndef DEFINE_RWF_SECTORS
extern const char *rdm_sector_str[ RDM_DOMAIN_COUNT ];
#else
const char *rdm_sector_str[ RDM_DOMAIN_COUNT ] = {
   0,
  "LOG"   /* login*/, 0, 0,
  "SRC"   /* source */,
  "DIC"   /* dictionary */,
  "REC"   /* market_price */,
  "MBO"   /* market_by_order */,
  "MBP"   /* market_by_price */,
  "MM"    /* market_maker */,
  "SL"    /* symbol_list */,
  "SPS"   /* service_provider_status */,
  "HIS"   /* history */,
  "HL"    /* headline */,
  "STO"   /* story */,
  "RHL"   /* replayheadline */,
  "RST"   /* replaystory */,
  "TRA"   /* transaction */, 0, 0, 0, 0,
  "YC"    /* yield_curve */, 0, 0, 0, 0,
  "CON"   /* contribution */, 0,
  "PA"    /* provider_admin */,
  "AN"    /* analytics */,
  "REF"   /* reference */, 0,
  "MRN"   /* news_text_analytics */,
  "EI"    /* economic_indicator */,
  "POL"   /* poll */,
  "FOR"   /* forecast */,
  "MBT"   /* market_by_time */
};
#endif

struct DictInProg {
  md::MDDictBuild dict_build;
  const char    * fld_dict_name,
                * enum_dict_name;
  uint32_t        field_stream_id,
                  enum_stream_id;
  uint8_t         dict_in_progress;
  void * operator new( size_t, void *ptr ) { return ptr; }
  void operator delete( void *ptr ) { ::free( ptr ); }
  DictInProg() : fld_dict_name( 0 ), enum_dict_name( 0 ),
                 field_stream_id( 0 ), enum_stream_id( 0 ),
                 dict_in_progress( 0 ) {}
  ~DictInProg() noexcept;
};

void print_dict_info( MDDict *d,  const char *fld_dict_name,
                      const char *enum_dict_name ) noexcept;
}
}
#endif
