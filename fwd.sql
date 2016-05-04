CREATE EXTENSION postgres_fdw;

/*  */
CREATE SERVER helpling_de
FOREIGN DATA WRAPPER postgres_fdw
OPTIONS (host 'ec2-52-48-254-195.eu-west-1.compute.amazonaws.com',
dbname 'dtdhe4ec4ktq8', port '5432', application_name 'bi_adames_fdw');

CREATE USER MAPPING for adames
SERVER helpling_de
OPTIONS (user '', password '');

CREATE FOREIGN TABLE users_de
(
  id character varying(255) NOT NULL,
  email character varying(255),
  sign_in_count integer,
  current_sign_in_at timestamp without time zone,
  last_sign_in_at timestamp without time zone,
  created_at timestamp without time zone,
  updated_at timestamp without time zone,
  account_type character varying(255),
  firstname character varying(255),
  lastname character varying(255),
  phone character varying(255),
  mobile character varying(255),
  unsubscribed boolean,
  terms boolean,
  terms_date timestamp without time zone,
  do_not_serve boolean,
  unsubscribed_at timestamp without time zone,
  metro character varying(255),
  country character varying(255),
  admin_role character varying(255),
  leave_reason character varying(255),
  leaver_date timestamp without time zone,
  deleted boolean,
  helpling_id character varying,
  work_status character varying(255),
  work_status_reason text,
  work_status_last_updated_at timestamp without time zone,
  work_status_last_updated_by character varying(255),
  customer_source character varying(255)
)
SERVER helpling_de
OPTIONS (table_name 'users');

select * from users_de limit 100;



DROP FOREIGN TABLE users_de;
DROP USER MAPPING IF EXISTS FOR adames SERVER helpling_de;
DROP SERVER helpling_de;