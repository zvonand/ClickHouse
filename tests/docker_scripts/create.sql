CREATE TABLE datasets.hits_v1
(
    WatchID UInt64,
    JavaEnable UInt8,
    Title String,
    GoodEvent Int16,
    EventTime DateTime,
    EventDate Date,
    CounterID UInt32,
    ClientIP UInt32,
    ClientIP6 FixedString(16),
    RegionID UInt32,
    UserID UInt64,
    CounterClass Int8,
    OS UInt8,
    UserAgent UInt8,
    URL String,
    Referer String,
    URLDomain String,
    RefererDomain String,
    Refresh UInt8,
    IsRobot UInt8,
    RefererCategories Array(UInt16),
    URLCategories Array(UInt16),
    URLRegions Array(UInt32),
    RefererRegions Array(UInt32),
    ResolutionWidth UInt16,
    ResolutionHeight UInt16,
    ResolutionDepth UInt8,
    FlashMajor UInt8,
    FlashMinor UInt8,
    FlashMinor2 String,
    NetMajor UInt8,
    NetMinor UInt8,
    UserAgentMajor UInt16,
    UserAgentMinor FixedString(2),
    CookieEnable UInt8,
    JavascriptEnable UInt8,
    IsMobile UInt8,
    MobilePhone UInt8,
    MobilePhoneModel String,
    Params String,
    IPNetworkID UInt32,
    TraficSourceID Int8,
    SearchEngineID UInt16,
    SearchPhrase String,
    AdvEngineID UInt8,
    IsArtifical UInt8,
    WindowClientWidth UInt16,
    WindowClientHeight UInt16,
    ClientTimeZone Int16,
    ClientEventTime DateTime,
    SilverlightVersion1 UInt8,
    SilverlightVersion2 UInt8,
    SilverlightVersion3 UInt32,
    SilverlightVersion4 UInt16,
    PageCharset String,
    CodeVersion UInt32,
    IsLink UInt8,
    IsDownload UInt8,
    IsNotBounce UInt8,
    FUniqID UInt64,
    HID UInt32,
    IsOldCounter UInt8,
    IsEvent UInt8,
    IsParameter UInt8,
    DontCountHits UInt8,
    WithHash UInt8,
    HitColor FixedString(1),
    UTCEventTime DateTime,
    Age UInt8,
    Sex UInt8,
    Income UInt8,
    Interests UInt16,
    Robotness UInt8,
    GeneralInterests Array(UInt16),
    RemoteIP UInt32,
    RemoteIP6 FixedString(16),
    WindowName Int32,
    OpenerName Int32,
    HistoryLength Int16,
    BrowserLanguage FixedString(2),
    BrowserCountry FixedString(2),
    SocialNetwork String,
    SocialAction String,
    HTTPError UInt16,
    SendTiming Int32,
    DNSTiming Int32,
    ConnectTiming Int32,
    ResponseStartTiming Int32,
    ResponseEndTiming Int32,
    FetchTiming Int32,
    RedirectTiming Int32,
    DOMInteractiveTiming Int32,
    DOMContentLoadedTiming Int32,
    DOMCompleteTiming Int32,
    LoadEventStartTiming Int32,
    LoadEventEndTiming Int32,
    NSToDOMContentLoadedTiming Int32,
    FirstPaintTiming Int32,
    RedirectCount Int8,
    SocialSourceNetworkID UInt8,
    SocialSourcePage String,
    ParamPrice Int64,
    ParamOrderID String,
    ParamCurrency FixedString(3),
    ParamCurrencyID UInt16,
    GoalsReached Array(UInt32),
    OpenstatServiceName String,
    OpenstatCampaignID String,
    OpenstatAdID String,
    OpenstatSourceID String,
    UTMSource String,
    UTMMedium String,
    UTMCampaign String,
    UTMContent String,
    UTMTerm String,
    FromTag String,
    HasGCLID UInt8,
    RefererHash UInt64,
    URLHash UInt64,
    CLID UInt32,
    YCLID UInt64,
    ShareService String,
    ShareURL String,
    ShareTitle String,
    "ParsedParams.Key1" Array(String),
    "ParsedParams.Key2" Array(String),
    "ParsedParams.Key3" Array(String),
    "ParsedParams.Key4" Array(String),
    "ParsedParams.Key5" Array(String),
    "ParsedParams.ValueDouble" Array(Float64),
    IslandID FixedString(16),
    RequestNum UInt32,
    RequestTry UInt8
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(EventDate)
ORDER BY (CounterID, EventDate, intHash32(UserID))
SAMPLE BY intHash32(UserID)
SETTINGS
    table_disk = 1,
    disk = disk(type = cache, path = 'filesystem_caches/stateful/', max_size = '4G',
    disk = disk(type = web, endpoint = 'https://clickhouse-datasets-web.s3.us-east-1.amazonaws.com/store/78e/78ebf6a1-d987-4579-b3ec-00c1a087b1f3/'));

CREATE TABLE datasets.visits_v1
(
    CounterID UInt32,
    StartDate Date,
    Sign Int8,
    IsNew UInt8,
    VisitID UInt64,
    UserID UInt64,
    StartTime DateTime,
    Duration UInt32,
    UTCStartTime DateTime,
    PageViews Int32,
    Hits Int32,
    IsBounce UInt8,
    Referer String,
    StartURL String,
    RefererDomain String,
    StartURLDomain String,
    EndURL String,
    LinkURL String,
    IsDownload UInt8,
    TraficSourceID Int8,
    SearchEngineID UInt16,
    SearchPhrase String,
    AdvEngineID UInt8,
    PlaceID Int32,
    RefererCategories Array(UInt16),
    URLCategories Array(UInt16),
    URLRegions Array(UInt32),
    RefererRegions Array(UInt32),
    IsYandex UInt8,
    GoalReachesDepth Int32,
    GoalReachesURL Int32,
    GoalReachesAny Int32,
    SocialSourceNetworkID UInt8,
    SocialSourcePage String,
    MobilePhoneModel String,
    ClientEventTime DateTime,
    RegionID UInt32,
    ClientIP UInt32,
    ClientIP6 FixedString(16),
    RemoteIP UInt32,
    RemoteIP6 FixedString(16),
    IPNetworkID UInt32,
    SilverlightVersion3 UInt32,
    CodeVersion UInt32,
    ResolutionWidth UInt16,
    ResolutionHeight UInt16,
    UserAgentMajor UInt16,
    UserAgentMinor UInt16,
    WindowClientWidth UInt16,
    WindowClientHeight UInt16,
    SilverlightVersion2 UInt8,
    SilverlightVersion4 UInt16,
    FlashVersion3 UInt16,
    FlashVersion4 UInt16,
    ClientTimeZone Int16,
    OS UInt8,
    UserAgent UInt8,
    ResolutionDepth UInt8,
    FlashMajor UInt8,
    FlashMinor UInt8,
    NetMajor UInt8,
    NetMinor UInt8,
    MobilePhone UInt8,
    SilverlightVersion1 UInt8,
    Age UInt8,
    Sex UInt8,
    Income UInt8,
    JavaEnable UInt8,
    CookieEnable UInt8,
    JavascriptEnable UInt8,
    IsMobile UInt8,
    BrowserLanguage UInt16,
    BrowserCountry UInt16,
    Interests UInt16,
    Robotness UInt8,
    GeneralInterests Array(UInt16),
    Params Array(String),
    "Goals.ID" Array(UInt32),
    "Goals.Serial" Array(UInt32),
    "Goals.EventTime" Array(DateTime),
    "Goals.Price" Array(Int64),
    "Goals.OrderID" Array(String),
    "Goals.CurrencyID" Array(UInt32),
    WatchIDs Array(UInt64),
    ParamSumPrice Int64,
    ParamCurrency FixedString(3),
    ParamCurrencyID UInt16,
    ClickLogID UInt64,
    ClickEventID Int32,
    ClickGoodEvent Int32,
    ClickEventTime DateTime,
    ClickPriorityID Int32,
    ClickPhraseID Int32,
    ClickPageID Int32,
    ClickPlaceID Int32,
    ClickTypeID Int32,
    ClickResourceID Int32,
    ClickCost UInt32,
    ClickClientIP UInt32,
    ClickDomainID UInt32,
    ClickURL String,
    ClickAttempt UInt8,
    ClickOrderID UInt32,
    ClickBannerID UInt32,
    ClickMarketCategoryID UInt32,
    ClickMarketPP UInt32,
    ClickMarketCategoryName String,
    ClickMarketPPName String,
    ClickAWAPSCampaignName String,
    ClickPageName String,
    ClickTargetType UInt16,
    ClickTargetPhraseID UInt64,
    ClickContextType UInt8,
    ClickSelectType Int8,
    ClickOptions String,
    ClickGroupBannerID Int32,
    OpenstatServiceName String,
    OpenstatCampaignID String,
    OpenstatAdID String,
    OpenstatSourceID String,
    UTMSource String,
    UTMMedium String,
    UTMCampaign String,
    UTMContent String,
    UTMTerm String,
    FromTag String,
    HasGCLID UInt8,
    FirstVisit DateTime,
    PredLastVisit Date,
    LastVisit Date,
    TotalVisits UInt32,
    "TraficSource.ID" Array(Int8),
    "TraficSource.SearchEngineID" Array(UInt16),
    "TraficSource.AdvEngineID" Array(UInt8),
    "TraficSource.PlaceID" Array(UInt16),
    "TraficSource.SocialSourceNetworkID" Array(UInt8),
    "TraficSource.Domain" Array(String),
    "TraficSource.SearchPhrase" Array(String),
    "TraficSource.SocialSourcePage" Array(String),
    Attendance FixedString(16),
    CLID UInt32,
    YCLID UInt64,
    NormalizedRefererHash UInt64,
    SearchPhraseHash UInt64,
    RefererDomainHash UInt64,
    NormalizedStartURLHash UInt64,
    StartURLDomainHash UInt64,
    NormalizedEndURLHash UInt64,
    TopLevelDomain UInt64,
    URLScheme UInt64,
    OpenstatServiceNameHash UInt64,
    OpenstatCampaignIDHash UInt64,
    OpenstatAdIDHash UInt64,
    OpenstatSourceIDHash UInt64,
    UTMSourceHash UInt64,
    UTMMediumHash UInt64,
    UTMCampaignHash UInt64,
    UTMContentHash UInt64,
    UTMTermHash UInt64,
    FromHash UInt64,
    WebVisorEnabled UInt8,
    WebVisorActivity UInt32,
    "ParsedParams.Key1" Array(String),
    "ParsedParams.Key2" Array(String),
    "ParsedParams.Key3" Array(String),
    "ParsedParams.Key4" Array(String),
    "ParsedParams.Key5" Array(String),
    "ParsedParams.ValueDouble" Array(Float64),
    "Market.Type" Array(UInt8),
    "Market.GoalID" Array(UInt32),
    "Market.OrderID" Array(String),
    "Market.OrderPrice" Array(Int64),
    "Market.PP" Array(UInt32),
    "Market.DirectPlaceID" Array(UInt32),
    "Market.DirectOrderID" Array(UInt32),
    "Market.DirectBannerID" Array(UInt32),
    "Market.GoodID" Array(String),
    "Market.GoodName" Array(String),
    "Market.GoodQuantity" Array(Int32),
    "Market.GoodPrice" Array(Int64),
    IslandID FixedString(16)
)
ENGINE = CollapsingMergeTree(Sign)
PARTITION BY toYYYYMM(StartDate)
ORDER BY (CounterID, StartDate, intHash32(UserID), VisitID)
SAMPLE BY intHash32(UserID)
SETTINGS
    table_disk = 1,
    disk = disk(type = cache, path = 'filesystem_caches/stateful/', max_size = '4G',
    disk = disk(type = web, endpoint = 'https://clickhouse-datasets-web.s3.us-east-1.amazonaws.com/store/513/5131f834-711f-4168-98a5-968b691a104b/'));

--- TPC-DS SF1 tables

SET data_type_default_nullable=1;

CREATE TABLE datasets.call_center
(
    cc_call_center_sk         Int64 NOT NULL,
    cc_call_center_id         FixedString(16) NOT NULL,
    cc_rec_start_date         Date,
    cc_rec_end_date           Date,
    cc_closed_date_sk         UInt32,
    cc_open_date_sk           UInt32,
    cc_name                   String,
    cc_class                  String,
    cc_employees              Int64,
    cc_sq_ft                  Int64,
    cc_hours                  FixedString(20),
    cc_manager                String,
    cc_mkt_id                 Int64,
    cc_mkt_class              FixedString(50),
    cc_mkt_desc               String,
    cc_market_manager         String,
    cc_division               Int64,
    cc_division_name          String,
    cc_company                Int64,
    cc_company_name           FixedString(50),
    cc_street_number          FixedString(10),
    cc_street_name            String,
    cc_street_type            FixedString(15),
    cc_suite_number           FixedString(10),
    cc_city                   String,
    cc_county                 String,
    cc_state                  FixedString(2),
    cc_zip                    FixedString(10),
    cc_country                String,
    cc_gmt_offset             Decimal(5,2),
    cc_tax_percentage         Decimal(5,2),
    PRIMARY KEY (cc_call_center_sk)
)
ENGINE = MergeTree
SETTINGS
    table_disk = 1,
    disk = disk(type = web, endpoint = 'https://tpc-ds-sf1.s3.amazonaws.com/call_center/');

SET data_type_default_nullable=1;

CREATE TABLE datasets.catalog_page
(
    cp_catalog_page_sk        Int64 NOT NULL,
    cp_catalog_page_id        FixedString(16) NOT NULL,
    cp_start_date_sk          UInt32,
    cp_end_date_sk            UInt32,
    cp_department             String,
    cp_catalog_number         Int64,
    cp_catalog_page_number    Int64,
    cp_description            String,
    cp_type                   String,
    PRIMARY KEY (cp_catalog_page_sk)
)
ENGINE = MergeTree
SETTINGS
    table_disk = 1,
    disk = disk(type = web, endpoint = 'https://tpc-ds-sf1.s3.amazonaws.com/catalog_page/');

SET data_type_default_nullable=1;

CREATE TABLE datasets.catalog_returns
(
    cr_returned_date_sk       UInt32,
    cr_returned_time_sk       UInt32,
    cr_item_sk                Int64 NOT NULL,
    cr_refunded_customer_sk   Int64,
    cr_refunded_cdemo_sk      Int64,
    cr_refunded_hdemo_sk      Int64,
    cr_refunded_addr_sk       Int64,
    cr_returning_customer_sk  Int64,
    cr_returning_cdemo_sk     Int64,
    cr_returning_hdemo_sk     Int64,
    cr_returning_addr_sk      Int64,
    cr_call_center_sk         Int64,
    cr_catalog_page_sk        Int64,
    cr_ship_mode_sk           Int64,
    cr_warehouse_sk           Int64,
    cr_reason_sk              Int64,
    cr_order_number           Int64 NOT NULL,
    cr_return_quantity        Int64,
    cr_return_amount          Decimal(7,2),
    cr_return_tax             Decimal(7,2),
    cr_return_amt_inc_tax     Decimal(7,2),
    cr_fee                    Decimal(7,2),
    cr_return_ship_cost       Decimal(7,2),
    cr_refunded_cash          Decimal(7,2),
    cr_reversed_charge        Decimal(7,2),
    cr_store_credit           Decimal(7,2),
    cr_net_loss               Decimal(7,2),
    PRIMARY KEY (cr_item_sk, cr_order_number)
)
ENGINE = MergeTree
SETTINGS
    table_disk = 1,
    disk = disk(type = web, endpoint = 'https://tpc-ds-sf1.s3.amazonaws.com/catalog_returns/');

SET data_type_default_nullable=1;

CREATE TABLE datasets.catalog_sales
(
    cs_sold_date_sk           UInt32,
    cs_sold_time_sk           UInt32,
    cs_ship_date_sk           UInt32,
    cs_bill_customer_sk       Int64,
    cs_bill_cdemo_sk          Int64,
    cs_bill_hdemo_sk          Int64,
    cs_bill_addr_sk           Int64,
    cs_ship_customer_sk       Int64,
    cs_ship_cdemo_sk          Int64,
    cs_ship_hdemo_sk          Int64,
    cs_ship_addr_sk           Int64,
    cs_call_center_sk         Int64,
    cs_catalog_page_sk        Int64,
    cs_ship_mode_sk           Int64,
    cs_warehouse_sk           Int64,
    cs_item_sk                Int64 NOT NULL,
    cs_promo_sk               Int64,
    cs_order_number           Int64 NOT NULL,
    cs_quantity               Int64,
    cs_wholesale_cost         Decimal(7,2),
    cs_list_price             Decimal(7,2),
    cs_sales_price            Decimal(7,2),
    cs_ext_discount_amt       Decimal(7,2),
    cs_ext_sales_price        Decimal(7,2),
    cs_ext_wholesale_cost     Decimal(7,2),
    cs_ext_list_price         Decimal(7,2),
    cs_ext_tax                Decimal(7,2),
    cs_coupon_amt             Decimal(7,2),
    cs_ext_ship_cost          Decimal(7,2),
    cs_net_paid               Decimal(7,2),
    cs_net_paid_inc_tax       Decimal(7,2),
    cs_net_paid_inc_ship      Decimal(7,2),
    cs_net_paid_inc_ship_tax  Decimal(7,2),
    cs_net_profit             Decimal(7,2),
    PRIMARY KEY (cs_item_sk, cs_order_number)
)
ENGINE = MergeTree
SETTINGS
    table_disk = 1,
    disk = disk(type = web, endpoint = 'https://tpc-ds-sf1.s3.amazonaws.com/catalog_sales/');

SET data_type_default_nullable=1;

CREATE TABLE datasets.customer_address
(
    ca_address_sk             Int64 NOT NULL,
    ca_address_id             FixedString(16) NOT NULL,
    ca_street_number          FixedString(10),
    ca_street_name            String,
    ca_street_type            FixedString(15),
    ca_suite_number           FixedString(10),
    ca_city                   String,
    ca_county                 String,
    ca_state                  FixedString(2),
    ca_zip                    FixedString(10),
    ca_country                String,
    ca_gmt_offset             Decimal(5,2),
    ca_location_type          FixedString(20),
    PRIMARY KEY (ca_address_sk)
)
ENGINE = MergeTree
SETTINGS
    table_disk = 1,
    disk = disk(type = web, endpoint = 'https://tpc-ds-sf1.s3.amazonaws.com/customer_address/');

SET data_type_default_nullable=1;

CREATE TABLE datasets.customer_demographics
(
    cd_demo_sk                Int64 NOT NULL,
    cd_gender                 FixedString(1),
    cd_marital_status         FixedString(1),
    cd_education_status       FixedString(20),
    cd_purchase_estimate      Int64,
    cd_credit_rating          FixedString(10),
    cd_dep_count              Int64,
    cd_dep_employed_count     Int64,
    cd_dep_college_count      Int64,
    PRIMARY KEY (cd_demo_sk)
)
ENGINE = MergeTree
SETTINGS
    table_disk = 1,
    disk = disk(type = web, endpoint = 'https://tpc-ds-sf1.s3.amazonaws.com/customer_demographics/');

SET data_type_default_nullable=1;

CREATE TABLE datasets.customer
(
    c_customer_sk             Int64 NOT NULL,
    c_customer_id             FixedString(16) NOT NULL,
    c_current_cdemo_sk        Int64,
    c_current_hdemo_sk        Int64,
    c_current_addr_sk         Int64,
    c_first_shipto_date_sk    UInt32,
    c_first_sales_date_sk     UInt32,
    c_salutation              FixedString(10),
    c_first_name              FixedString(20),
    c_last_name               FixedString(30),
    c_preferred_cust_flag     FixedString(1),
    c_birth_day               Int64,
    c_birth_month             Int64,
    c_birth_year              Int64,
    c_birth_country           String,
    c_login                   FixedString(13),
    c_email_address           FixedString(50),
    c_last_review_date_sk     UInt32,
    PRIMARY KEY (c_customer_sk)
)
ENGINE = MergeTree
SETTINGS
    table_disk = 1,
    disk = disk(type = web, endpoint = 'https://tpc-ds-sf1.s3.amazonaws.com/customer/');

SET data_type_default_nullable=1;

CREATE TABLE datasets.date_dim
(
    d_date_sk                 UInt32 NOT NULL,
    d_date_id                 FixedString(16) NOT NULL,
    d_date                    Date NOT NULL,
    d_month_seq               Int64,
    d_week_seq                Int64,
    d_quarter_seq             Int64,
    d_year                    Int64,
    d_dow                     Int64,
    d_moy                     Int64,
    d_dom                     Int64,
    d_qoy                     Int64,
    d_fy_year                 Int64,
    d_fy_quarter_seq          Int64,
    d_fy_week_seq             Int64,
    d_day_name                FixedString(9),
    d_quarter_name            FixedString(6),
    d_holiday                 FixedString(1),
    d_weekend                 FixedString(1),
    d_following_holiday       FixedString(1),
    d_first_dom               Int64,
    d_last_dom                Int64,
    d_same_day_ly             Int64,
    d_same_day_lq             Int64,
    d_current_day             FixedString(1),
    d_current_week            FixedString(1),
    d_current_month           FixedString(1),
    d_current_quarter         FixedString(1),
    d_current_year            FixedString(1),
    PRIMARY KEY (d_date_sk)
)
ENGINE = MergeTree
SETTINGS
    table_disk = 1,
    disk = disk(type = web, endpoint = 'https://tpc-ds-sf1.s3.amazonaws.com/date_dim/');

SET data_type_default_nullable=1;

CREATE TABLE datasets.household_demographics
(
    hd_demo_sk                Int64 NOT NULL,
    hd_income_band_sk         Int64,
    hd_buy_potential          FixedString(15),
    hd_dep_count              Int64,
    hd_vehicle_count          Int64,
    PRIMARY KEY (hd_demo_sk)
)
ENGINE = MergeTree
SETTINGS
    table_disk = 1,
    disk = disk(type = web, endpoint = 'https://tpc-ds-sf1.s3.amazonaws.com/household_demographics/');

SET data_type_default_nullable=1;

CREATE TABLE datasets.income_band
(
    ib_income_band_sk         Int64 NOT NULL,
    ib_lower_bound            Int64,
    ib_upper_bound            Int64,
    PRIMARY KEY (ib_income_band_sk)
)
ENGINE = MergeTree
SETTINGS
    table_disk = 1,
    disk = disk(type = web, endpoint = 'https://tpc-ds-sf1.s3.amazonaws.com/income_band/');

SET data_type_default_nullable=1;

CREATE TABLE datasets.inventory
(
    inv_date_sk             UInt32 NOT NULL,
    inv_item_sk             Int64 NOT NULL,
    inv_warehouse_sk        Int64 NOT NULL,
    inv_quantity_on_hand    Int64,
    PRIMARY KEY (inv_date_sk, inv_item_sk, inv_warehouse_sk)
)
ENGINE = MergeTree
SETTINGS
    table_disk = 1,
    disk = disk(type = web, endpoint = 'https://tpc-ds-sf1.s3.amazonaws.com/inventory/');

SET data_type_default_nullable=1;

CREATE TABLE datasets.item
(
    i_item_sk                 Int64 NOT NULL,
    i_item_id                 FixedString(16) NOT NULL,
    i_rec_start_date          Date,
    i_rec_end_date            Date,
    i_item_desc               String,
    i_current_price           Decimal(7,2),
    i_wholesale_cost          Decimal(7,2),
    i_brand_id                Int64,
    i_brand                   FixedString(50),
    i_class_id                Int64,
    i_class                   FixedString(50),
    i_category_id             Int64,
    i_category                FixedString(50),
    i_manufact_id             Int64,
    i_manufact                FixedString(50),
    i_size                    FixedString(20),
    i_formulation             FixedString(20),
    i_color                   FixedString(20),
    i_units                   FixedString(10),
    i_container               FixedString(10),
    i_manager_id              Int64,
    i_product_name            FixedString(50),
    PRIMARY KEY (i_item_sk)
)
ENGINE = MergeTree
SETTINGS
    table_disk = 1,
    disk = disk(type = web, endpoint = 'https://tpc-ds-sf1.s3.amazonaws.com/item/');

SET data_type_default_nullable=1;

CREATE TABLE datasets.promotion
(
    p_promo_sk                Int64 NOT NULL,
    p_promo_id                FixedString(16) NOT NULL,
    p_start_date_sk           UInt32,
    p_end_date_sk             UInt32,
    p_item_sk                 Int64,
    p_cost                    Decimal(15,2),
    p_response_target         Int64,
    p_promo_name              FixedString(50),
    p_channel_dmail           FixedString(1),
    p_channel_email           FixedString(1),
    p_channel_catalog         FixedString(1),
    p_channel_tv              FixedString(1),
    p_channel_radio           FixedString(1),
    p_channel_press           FixedString(1),
    p_channel_event           FixedString(1),
    p_channel_demo            FixedString(1),
    p_channel_details         String,
    p_purpose                 FixedString(15),
    p_discount_active         FixedString(1),
    PRIMARY KEY (p_promo_sk)
)
ENGINE = MergeTree
SETTINGS
    table_disk = 1,
    disk = disk(type = web, endpoint = 'https://tpc-ds-sf1.s3.amazonaws.com/promotion/');

SET data_type_default_nullable=1;

CREATE TABLE datasets.reason
(
    r_reason_sk               Int64 NOT NULL,
    r_reason_id               FixedString(16) NOT NULL,
    r_reason_desc             FixedString(100),
    PRIMARY KEY (r_reason_sk)
)
ENGINE = MergeTree
SETTINGS
    table_disk = 1,
    disk = disk(type = web, endpoint = 'https://tpc-ds-sf1.s3.amazonaws.com/reason/');

SET data_type_default_nullable=1;

CREATE TABLE datasets.ship_mode
(
    sm_ship_mode_sk           Int64 NOT NULL,
    sm_ship_mode_id           FixedString(16) NOT NULL,
    sm_type                   FixedString(30),
    sm_code                   FixedString(10),
    sm_carrier                FixedString(20),
    sm_contract               FixedString(20),
    PRIMARY KEY (sm_ship_mode_sk)
)
ENGINE = MergeTree
SETTINGS
    table_disk = 1,
    disk = disk(type = web, endpoint = 'https://tpc-ds-sf1.s3.amazonaws.com/ship_mode/');

SET data_type_default_nullable=1;

CREATE TABLE datasets.store_returns
(
    sr_returned_date_sk       UInt32,
    sr_return_time_sk         UInt32,
    sr_item_sk                Int64 NOT NULL,
    sr_customer_sk            Int64,
    sr_cdemo_sk               Int64,
    sr_hdemo_sk               Int64,
    sr_addr_sk                Int64,
    sr_store_sk               Int64,
    sr_reason_sk              Int64,
    sr_ticket_number          Int64 NOT NULL,
    sr_return_quantity        Int64,
    sr_return_amt             Decimal(7,2),
    sr_return_tax             Decimal(7,2),
    sr_return_amt_inc_tax     Decimal(7,2),
    sr_fee                    Decimal(7,2),
    sr_return_ship_cost       Decimal(7,2),
    sr_refunded_cash          Decimal(7,2),
    sr_reversed_charge        Decimal(7,2),
    sr_store_credit           Decimal(7,2),
    sr_net_loss               Decimal(7,2),
    PRIMARY KEY (sr_item_sk, sr_ticket_number)
)
ENGINE = MergeTree
SETTINGS
    table_disk = 1,
    disk = disk(type = web, endpoint = 'https://tpc-ds-sf1.s3.amazonaws.com/store_returns/');

SET data_type_default_nullable=1;

CREATE TABLE datasets.store_sales
(
    ss_sold_date_sk           UInt32,
    ss_sold_time_sk           UInt32,
    ss_item_sk                Int64 NOT NULL,
    ss_customer_sk            Int64,
    ss_cdemo_sk               Int64,
    ss_hdemo_sk               Int64,
    ss_addr_sk                Int64,
    ss_store_sk               Int64,
    ss_promo_sk               Int64,
    ss_ticket_number          Int64 NOT NULL,
    ss_quantity               Int64,
    ss_wholesale_cost         Decimal(7,2),
    ss_list_price             Decimal(7,2),
    ss_sales_price            Decimal(7,2),
    ss_ext_discount_amt       Decimal(7,2),
    ss_ext_sales_price        Decimal(7,2),
    ss_ext_wholesale_cost     Decimal(7,2),
    ss_ext_list_price         Decimal(7,2),
    ss_ext_tax                Decimal(7,2),
    ss_coupon_amt             Decimal(7,2),
    ss_net_paid               Decimal(7,2),
    ss_net_paid_inc_tax       Decimal(7,2),
    ss_net_profit             Decimal(7,2),
    PRIMARY KEY (ss_item_sk, ss_ticket_number)
)
ENGINE = MergeTree
SETTINGS
    table_disk = 1,
    disk = disk(type = web, endpoint = 'https://tpc-ds-sf1.s3.amazonaws.com/store_sales/');

SET data_type_default_nullable=1;

CREATE TABLE datasets.store
(
    s_store_sk                Int64 NOT NULL,
    s_store_id                FixedString(16) NOT NULL,
    s_rec_start_date          Date,
    s_rec_end_date            Date,
    s_closed_date_sk          UInt32,
    s_store_name              String,
    s_number_employees        Int64,
    s_floor_space             Int64,
    s_hours                   FixedString(20),
    s_manager                 String,
    s_market_id               Int64,
    s_geography_class         String,
    s_market_desc             String,
    s_market_manager          String,
    s_division_id             Int64,
    s_division_name           String,
    s_company_id              Int64,
    s_company_name            String,
    s_street_number           String,
    s_street_name             String,
    s_street_type             FixedString(15),
    s_suite_number            FixedString(10),
    s_city                    String,
    s_county                  String,
    s_state                   FixedString(2),
    s_zip                     FixedString(10),
    s_country                 String,
    s_gmt_offset              Decimal(5,2),
    s_tax_percentage          Decimal(5,2),
    PRIMARY KEY (s_store_sk)
)
ENGINE = MergeTree
SETTINGS
    table_disk = 1,
    disk = disk(type = web, endpoint = 'https://tpc-ds-sf1.s3.amazonaws.com/store/');

SET data_type_default_nullable=1;

CREATE TABLE datasets.time_dim
(
    t_time_sk                 UInt32 NOT NULL,
    t_time_id                 FixedString(16) NOT NULL,
    t_time                    Int64 NOT NULL,
    t_hour                    Int64,
    t_minute                  Int64,
    t_second                  Int64,
    t_am_pm                   FixedString(2),
    t_shift                   FixedString(20),
    t_sub_shift               FixedString(20),
    t_meal_time               FixedString(20),
    PRIMARY KEY (t_time_sk)
)
ENGINE = MergeTree
SETTINGS
    table_disk = 1,
    disk = disk(type = web, endpoint = 'https://tpc-ds-sf1.s3.amazonaws.com/time_dim/');

SET data_type_default_nullable=1;

CREATE TABLE datasets.warehouse
(
    w_warehouse_sk            Int64 NOT NULL,
    w_warehouse_id            FixedString(16) NOT NULL,
    w_warehouse_name          String,
    w_warehouse_sq_ft         Int64,
    w_street_number           FixedString(10),
    w_street_name             String,
    w_street_type             FixedString(15),
    w_suite_number            FixedString(10),
    w_city                    String,
    w_county                  String,
    w_state                   FixedString(2),
    w_zip                     FixedString(10),
    w_country                 String,
    w_gmt_offset              Decimal(5,2),
    PRIMARY KEY (w_warehouse_sk)
)
ENGINE = MergeTree
SETTINGS
    table_disk = 1,
    disk = disk(type = web, endpoint = 'https://tpc-ds-sf1.s3.amazonaws.com/warehouse/');

SET data_type_default_nullable=1;

CREATE TABLE datasets.web_page
(
    wp_web_page_sk            Int64 NOT NULL,
    wp_web_page_id            FixedString(16) NOT NULL,
    wp_rec_start_date         Date,
    wp_rec_end_date           Date,
    wp_creation_date_sk       UInt32,
    wp_access_date_sk         UInt32,
    wp_autogen_flag           FixedString(1),
    wp_customer_sk            Int64,
    wp_url                    String,
    wp_type                   FixedString(50),
    wp_char_count             Int64,
    wp_link_count             Int64,
    wp_image_count            Int64,
    wp_max_ad_count           Int64,
    PRIMARY KEY (wp_web_page_sk)
)
ENGINE = MergeTree
SETTINGS
    table_disk = 1,
    disk = disk(type = web, endpoint = 'https://tpc-ds-sf1.s3.amazonaws.com/web_page/');

SET data_type_default_nullable=1;

CREATE TABLE datasets.web_returns
(
    wr_returned_date_sk       UInt32,
    wr_returned_time_sk       UInt32,
    wr_item_sk                Int64 NOT NULL,
    wr_refunded_customer_sk   Int64,
    wr_refunded_cdemo_sk      Int64,
    wr_refunded_hdemo_sk      Int64,
    wr_refunded_addr_sk       Int64,
    wr_returning_customer_sk  Int64,
    wr_returning_cdemo_sk     Int64,
    wr_returning_hdemo_sk     Int64,
    wr_returning_addr_sk      Int64,
    wr_web_page_sk            Int64,
    wr_reason_sk              Int64,
    wr_order_number           Int64 NOT NULL,
    wr_return_quantity        Int64,
    wr_return_amt             Decimal(7,2),
    wr_return_tax             Decimal(7,2),
    wr_return_amt_inc_tax     Decimal(7,2),
    wr_fee                    Decimal(7,2),
    wr_return_ship_cost       Decimal(7,2),
    wr_refunded_cash          Decimal(7,2),
    wr_reversed_charge        Decimal(7,2),
    wr_account_credit         Decimal(7,2),
    wr_net_loss               Decimal(7,2),
    PRIMARY KEY (wr_item_sk, wr_order_number)
)
ENGINE = MergeTree
SETTINGS
    table_disk = 1,
    disk = disk(type = web, endpoint = 'https://tpc-ds-sf1.s3.amazonaws.com/web_returns/');

SET data_type_default_nullable=1;

CREATE TABLE datasets.web_sales
(
    ws_sold_date_sk           UInt32,
    ws_sold_time_sk           UInt32,
    ws_ship_date_sk           UInt32,
    ws_item_sk                Int64 NOT NULL,
    ws_bill_customer_sk       Int64,
    ws_bill_cdemo_sk          Int64,
    ws_bill_hdemo_sk          Int64,
    ws_bill_addr_sk           Int64,
    ws_ship_customer_sk       Int64,
    ws_ship_cdemo_sk          Int64,
    ws_ship_hdemo_sk          Int64,
    ws_ship_addr_sk           Int64,
    ws_web_page_sk            Int64,
    ws_web_site_sk            Int64,
    ws_ship_mode_sk           Int64,
    ws_warehouse_sk           Int64,
    ws_promo_sk               Int64,
    ws_order_number           Int64 NOT NULL,
    ws_quantity               Int64,
    ws_wholesale_cost         Decimal(7,2),
    ws_list_price             Decimal(7,2),
    ws_sales_price            Decimal(7,2),
    ws_ext_discount_amt       Decimal(7,2),
    ws_ext_sales_price        Decimal(7,2),
    ws_ext_wholesale_cost     Decimal(7,2),
    ws_ext_list_price         Decimal(7,2),
    ws_ext_tax                Decimal(7,2),
    ws_coupon_amt             Decimal(7,2),
    ws_ext_ship_cost          Decimal(7,2),
    ws_net_paid               Decimal(7,2),
    ws_net_paid_inc_tax       Decimal(7,2),
    ws_net_paid_inc_ship      Decimal(7,2),
    ws_net_paid_inc_ship_tax  Decimal(7,2),
    ws_net_profit             Decimal(7,2),
    PRIMARY KEY (ws_item_sk, ws_order_number)
)
ENGINE = MergeTree
SETTINGS
    table_disk = 1,
    disk = disk(type = web, endpoint = 'https://tpc-ds-sf1.s3.amazonaws.com/web_sales/');

SET data_type_default_nullable=1;

CREATE TABLE datasets.web_site
(
    web_site_sk           Int64 NOT NULL,
    web_site_id           FixedString(16) NOT NULL,
    web_rec_start_date    Date,
    web_rec_end_date      Date,
    web_name              String,
    web_open_date_sk      UInt32,
    web_close_date_sk     UInt32,
    web_class             String,
    web_manager           String,
    web_mkt_id            Int64,
    web_mkt_class         String,
    web_mkt_desc          String,
    web_market_manager    String,
    web_company_id        Int64,
    web_company_name      FixedString(50),
    web_street_number     FixedString(10),
    web_street_name       String,
    web_street_type       FixedString(15),
    web_suite_number      FixedString(10),
    web_city              String,
    web_county            String,
    web_state             FixedString(2),
    web_zip               FixedString(10),
    web_country           String,
    web_gmt_offset        Decimal(5,2),
    web_tax_percentage    Decimal(5,2),
    PRIMARY KEY (web_site_sk)
)
ENGINE = MergeTree
SETTINGS
    table_disk = 1,
    disk = disk(type = web, endpoint = 'https://tpc-ds-sf1.s3.amazonaws.com/web_site/');
