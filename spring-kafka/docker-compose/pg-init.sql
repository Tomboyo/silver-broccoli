create table if not exists audit_log(
  id serial primary key,
  message varchar(255) unique not null
);