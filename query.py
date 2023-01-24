QUERY = """
with 
----------------------------------------------------------------------------------------------------
---------------------- Org Name Lookups ------------------------------------------------------------
----------------------------------------------------------------------------------------------------
org_names as 
(
    select 
    to_address as org_addr,
    try_hex_decode_string(regexp_substr_all(SUBSTR(input, 11, len(input)), '.{64}')[14]::string) AS org_name
    from 
    polygon.core.fact_traces
    where block_timestamp :: DATE > '2022-10-16' :: DATE
    and type = 'CALL'
    and from_address = LOWER('0x218b3c623ffb9c5e4dbb9142e6ca6f6559f1c2d6') -- deployer 
    and substr(input, 0, 10) = '0xb6dbcae5'
),
----------------------------------------------------------------------------------------------------
---------------------- _Base_Table: Token Definion & Actions ----------------------------------------
----------------------------------------------------------------------------------------------------
-- base table for token actions
_base_table AS 
(
    SELECT
    block_timestamp,
    block_number,
    event_index,
    contract_address AS org_addr,
    CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS from_address,
    CONCAT('0x', SUBSTR(topics [3] :: STRING, 27, 40)) AS to_address,
    CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS OPERATOR,
    regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
    ethereum.public.udf_hex_to_int(segmented_data [0] :: STRING) AS id,
    ethereum.public.udf_hex_to_int(segmented_data [1] :: STRING) AS value_erc1155
    FROM
    polygon.core.fact_event_logs
    WHERE block_timestamp :: DATE > '2022-10-16' :: DATE
    AND topics [0] :: STRING = '0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62'
    AND contract_address in (select distinct org_addr from org_names)
    -- AND contract_address = lower('0x74539714796c0facc5ea32fb180f7cb04c71e97f') -- <-- PARAM FOR ORG

),
----------------------------------------------------------------------------------------------------
---------------------- Insert Name into _Base_Table = Base_Table -----------------------------------
----------------------------------------------------------------------------------------------------
base_table as 
(
    select 
    block_timestamp,
    block_number,
    event_index,
    _base_table.org_addr,
    trim(org_name) as org_name,
    from_address,
    to_address,
    operator,
    segmented_data,
    id,
    value_erc1155
    from 
    _base_table
    left join org_names on _base_table.org_addr = org_names.org_addr
),
----------------------------------------------------------------------------------------------------
---------------------- Use Base_Table --------------------------------------------------------------
----------------------------------------------------------------------------------------------------
mints AS (
    SELECT
    block_timestamp,
    org_name,
    to_address as target
    FROM
    base_table
    WHERE from_address = '0x0000000000000000000000000000000000000000'
),
burns AS (
    SELECT
    block_timestamp,
    org_name,
    from_address as source
    FROM
    base_table
    WHERE to_address = '0x0000000000000000000000000000000000000000'
),
----------------------------------------------------------------------------------------------------
---------------------- User ENS Lookups ------------------------------------------------------------
----------------------------------------------------------------------------------------------------
-- ens lookups
user_ens as 
(
  select 
  owner,
  ens_name as name
  from 
  crosschain.core.ez_ens
  where owner in (select distinct target from mints)
  and ens_set = 'Y'
)
select 
replace(org_name,',') as source,
coalesce(name,target) as target,
block_timestamp::date as date
FROM
mints
left join user_ens on mints.target = user_ens.owner
union all 
select 
replace(coalesce(name,source),',') as source,
'Burn' as target,
block_timestamp::date as date
FROM
burns
left join user_ens on burns.source = user_ens.owner
order by date
"""







