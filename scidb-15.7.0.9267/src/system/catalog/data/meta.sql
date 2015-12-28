--
-- BEGIN_COPYRIGHT
--
-- Copyright (C) 2008-2015 SciDB, Inc.
-- All Rights Reserved.
--
-- This file is part of the Paradigm4 Enterprise SciDB distribution kit
-- and may only be used with a valid Paradigm4 contract and in accord
-- with the terms and conditions specified by that contract.
--
-- END_COPYRIGHT
--
-- CLEAR
--

-- ---------------------------------------------------------------------
-- DROP TABLES
-- ---------------------------------------------------------------------
drop table if exists "array" cascade;
drop table if exists "array_version" cascade;
drop table if exists "array_version_lock" cascade;
drop table if exists "array_partition" cascade;
drop table if exists "instance" cascade;
drop table if exists "array_attribute" cascade;
drop table if exists "array_dimension" cascade;
drop table if exists "cluster" cascade;
drop table if exists "libraries" cascade;
drop table if exists "users" cascade;
drop table if exists "namespaces" cascade;
drop table if exists "namespace_members" cascade;

-- ---------------------------------------------------------------------
-- DROP SEQUENCES
-- ---------------------------------------------------------------------
drop sequence if exists "array_id_seq" cascade;
drop sequence if exists "partition_id_seq" cascade;
drop sequence if exists "instance_id_seq" cascade;
drop sequence if exists "libraries_id_seq" cascade;
drop sequence if exists "user_id_seq" cascade;
drop sequence if exists "namespaces_id_seq" cascade;

-- ---------------------------------------------------------------------
-- DROP FUNCTIONS
-- ---------------------------------------------------------------------
drop function if exists uuid_generate_v1();
drop function if exists get_cluster_uuid();
drop function if exists get_metadata_version();

-- ---------------------------------------------------------------------
-- CREATE SEQUENCES
-- ---------------------------------------------------------------------
create sequence "array_id_seq";
create sequence "partition_id_seq";
create sequence "instance_id_seq" minvalue 0 start with 0;
create sequence "libraries_id_seq";
create sequence "user_id_seq";
create sequence "namespaces_id_seq" minvalue 1 start with 2;

-- ---------------------------------------------------------------------
-- CREATE TABLES
-- ---------------------------------------------------------------------
create table "cluster"
(
  cluster_uuid uuid,
  metadata_version integer
);
--
--  Table: "array"  (public.array) List of arrays in the SciDB installation.
--
--          Information about all persistent arrays in the SciDB installation
--          are recorded in public.array.
--
--   SciDB arrays recorded in public.array come in several forms.
--
--   1. Basic (or persistent) Arrays: Arrays named in CREATE ARRAY statements.
--
--   2. Array Versions: SciDB supports data modification by creating new
--      version of basic arrays. Each new version of a Basic array gets its
--      own entry in public.array.
--
--   SciDB creates rows in the public.array catalog to record the existance
--  of many things; arrays created by users, each version of these arrays,
--  and arrays created to hold non-integer dimension values and their
--  mappings to array dimension offsets.
--
--   public.array.id - unique int64 identifier for the array. This synthetic
--            key is used to maintain references within the catalogs, and to
--            identify the array within the SciDB engine's own metadata:
--            ArrayDesc._id.
--
-- public.array.name - the array's name. If the array is a Basic or persistent
--            array the public.array.name reflects the name as it appears in
--            the CREATE ARRAY statement. If the entry corresponds to a
--            version of an array, then the contents of this field consist of
--            the array name plus the version number.
--
--public.array.partitiong_scheme - SciDB supports several partitioning schemes.
--
--                     0. Replication of the array's contents on all instances.
--                     1. Round Robin allocation of chunks to instances.
--
-- public.array.flags - records details about the array's status.
--
--
create table "array"
(
  id bigint primary key default nextval('array_id_seq'),
  name varchar unique,
  partitioning_schema integer,
  flags integer
);
--
--   Table: public.array_version
--
--      Information about array versions, their relationship to the Basic
--   arrays and the entries in the public.array table, and their creation
--   timestamps.
--
--  public.array_version.array_id - reference back to the entry in the
--                                  public.array.id column that corresponds
--                                  to the Basic array.
--
--  public.array_version.version_id - the (sequential) number of the version.
--
--  public.array_version.version_array_id - reference back to the entry in the
--                  public.array.id column that identifies the Versioned Array.
--
--  public.array_version.time_stamp - timestamp (in seconds since epoch) at
--                  which the version was created.
--
--  PGB: I worry that time-to-the-second might not be sufficient precision.
--
create table "array_version"
(
   array_id bigint references "array" (id) on delete cascade,
   version_id bigint,
   version_array_id bigint references "array" (id) on delete cascade,
   time_stamp bigint,
   primary key(array_id, time_stamp, version_id),
   unique(array_id, version_id)
);
--
--  Table: public.array_version_lock
--
--    Information about the locks held by currently running queries.
--
create table "array_version_lock"
(
   array_name varchar,
   array_id bigint,
   query_id bigint,
   instance_id bigint,
   array_version_id bigint,
   array_version bigint,
   instance_role integer, -- 0-invalid, 1-coordinator, 2-worker
   lock_mode integer, -- {0=invalid, read, write, remove, renameto, renamefrom}
   unique(array_name, query_id, instance_id)
);
--
--  Table: public.instance
--
--    Information about the SciDB instances that are part of this installation.
--
create table "instance"
(
  instance_id bigint primary key default nextval('instance_id_seq'),
  host varchar,
  port integer,
  online_since timestamp,
  path varchar
);
--
--  Table: public.array_partition
--
--    Information about the way arrays are mapped to instances.
--
--    NOTE: Currently unused.
--
create table "array_partition"
(
  partition_id bigint primary key default nextval('partition_id_seq'),
  array_id bigint references "array" (id) on delete cascade,
  partition_function varchar,
  instance_id integer references "instance" (instance_id) on delete cascade
);
--
--  Table: public.array_attribute
--
--     Information about each array's attributes.
--
--  public.array_attribute.array_id - reference to the entry in the
--          public.array catalog containing details about the array to which
--          this attribute belongs.
--
--          Each new array version creates an entirely new set of entries in
--          the public.array_attribute and the public.array_dimension catalogs.
--
--  public.array_attribute.id - defines the order of the attribute within the
--                              array.
--
--  public.array_attribute.name - name of the attribute as it appears in the
--           CREATE ARRAY ... statement.
--
--  public.array_attribute.type - data type of the attribute. Note that the
--           types are not recorded in the catalogs, but instead are named
--           when the types are loaded into each instance. The idea is that
--           using integer identifiers would make it hard to disentangle
--           changes in type implementation.
--
--  public.array_attribute.flags - information about the attribute.
--
--  public.array_attribute.default_compression_method - compression method
--            used on the attribute's chunks.
--
--  public.array_attribute.reserve - SciDB organizes attribute data into
--            chunks, which are the physical unit of storage and
--            inter-instance communication. To support modifications to the
--            array each chunk reserves a block of memory to hold backwards
--            deltas that can be applied to the chunk's contents to recover
--            previous versions. The value in this column reflects the
--            percentage of the chunk which is set aside (reserved) to hold
--            deltas.
--
-- public.array_attribute.default_missing_reason - if the attribute has a
--            DEFAULT with a missing code, the missing code is recorded here.
--            If the attribute has no default missing code (and by default,
--            SciDB array attributes are prevented from having missing codes).
--
-- public.array_attribute.default_value - if the attribute has a DEFAULT value,
--            the default value is recorded here. When a DEFAULT value is
--            calculated from an expression, the expression is placed here.
--
create table "array_attribute"
(
  array_id bigint references "array" (id) on delete cascade,
  id int,
  name varchar not null,
  type varchar,
  flags int,
  default_compression_method int,
  reserve int,
  default_missing_reason int not null,
  default_value varchar null,
  primary key(array_id, id),
  unique ( array_id, name )
);
--
-- Table: public.array_dimension
--
--    Information about array dimensions.
--
--  Array dimensions come in three forms.
--
--  1. There are integer (whole number) dimensions that correspond to the
--     simplest and most basic arrays.
--
--  2. There are non-integer dimensions where the values that make up the
--     collection of labels on the dimension are organized into an array.
--
--  3. There are non-integer dimensions where a pair of functions are used
--     to map from the dimension's type to int64 values, and vice-versa.
--
--  public.array_dimension.array_id - reference to the entry in the
--                          public.array catalog containing details about
--                          the array to which this dimension belongs.
--
--                           Each new array version creates an entirely new
--                          set of entries in the public.array_attribute and
--                          the public.array_dimension catalogs.
--
--  public.array_dimension.id - order of the dimension within the array's
--                              shape. The combination of the array id and
--                              this id make up the key of this catalog.
--
--  public.array_dimension.name - the name of the array dimension.
--
--  public.array_dimension.startMin
--                        .currStart
--                        .currEnd
--                        .endMax
--
--     Regardless of the data types used in the dimension, all array
--   dimensions have an extent defined in terms of the integer offsets into
--   the dimension space. Generally, array dimensions have an initial offset
--   of zero (0), but then some maximum length (say 100). Now, it is also true
--   that when they are declared, array dimensions can be given proscribed
--   limits. For example, we can declare that an array can only be 200 long
--   on some dimension. Also, arrays can be unbounded in their declaration,
--   but in practice can currently have only a current length.
--
--    These four columns of the public.array_dimension catalog record the
--   minimum legal start value, the current start value, the current end value
--   and the maximum end value for the dimension. The dimension's length is
--   currEnd - currStart.
--
--  public.array_dimension.chunk_interval - length of the chunks in this
--               dimension.
--
--  public.array_dimension.chunk_overlap  - overlap of the chunks in this
--               dimension.
--
--  public.array_dimension.type - name of the data type for the dimension, if
--               the dimension is declared as a non-integer type.
--
--
create table "array_dimension"
(
  array_id bigint references "array" (id) on delete cascade,
  id int,
  name varchar not null,
  startMin bigint,
  currStart bigint,
  currEnd bigint,
  endMax bigint,
--
  chunk_interval bigint,
  chunk_overlap bigint,
  primary key(array_id, id),
  unique ( array_id, name )
);

create table "libraries"
(
  id bigint primary key default nextval('libraries_id_seq'),
  name varchar unique
);


--
--  Table: public.users
--
--  Maps a SciDB user to their id and password.
--
--  public.users.id - The (sequential) number of the user
--
--  public.users.name - The user's name
--
--  public.users.password - The user's password
--
--
create table "users"
(
  id bigint primary key default nextval('user_id_seq'),
  name varchar unique,
  password varchar,
  method varchar
);

--
--  Table: public.namespaces
--
--  Maps a SciDB namespace to its id.
--
--  public.namespaces.id - The (sequential) number of the
--      namespace
--
--  public.namespaces.name - The name of the namespace
--
--
create table "namespaces"
(
  id bigint primary key default nextval('namespaces_id_seq'),
  name varchar unique
);

--
--  Table: public.namespace_members
--
--  Maps a SciDB namespace to its corresponding arrays
--
--  public.namespaces.namespace_id - The (sequential) number of the
--      namespace
--
--  public.namespaces.array_id - The array.id in the namespace
--
--
create table "namespace_members"
(
  namespace_id bigint references "namespaces" (id) on delete cascade,
  array_id bigint references "array" (id) on delete cascade,

  primary key(namespace_id, array_id)
);


create or replace function uuid_generate_v1()
returns uuid
as '$libdir/uuid-ossp', 'uuid_generate_v1'
volatile strict language C;


-- The version number (3) corresponds to the var METADATA_VERSION from Constants.h
-- If we start and find that cluster.metadata_version is less than METADATA_VERSION
-- upgrade. The upgrade files are provided as sql scripts in
-- src/system/catalog/data/[NUMBER].sql. They are converted to string
-- constants in a C++ header file  (variable METADATA_UPGRADES_LIST[])
-- and then linked in at build time.
-- @see SystemCatalog::connect(const string&, bool)
-- Note: there is no downgrade path at the moment.
insert into "cluster" values (uuid_generate_v1(), 3);

insert into users (name, password, method) values (
    'root',
    'eUCUk3B57IVO9ZfJB6CIEHl/0lxrWg/7PV8KytUNY6kPLhTX2db48GHGHoizKyH+\nuGkCfNTYZrJgKzjWOhjuvg==',
    'raw');
insert into namespaces (name, id) values ('public', 1);


create function get_cluster_uuid() returns uuid as $$
declare t uuid;
begin
  select into t cluster_uuid from "cluster" limit 1;
  return t;
end;
$$ language plpgsql;

create function get_metadata_version() returns integer as $$
declare v integer;
begin
  select into v metadata_version from "cluster" limit 1;
  return v;
end;
$$ language plpgsql;


